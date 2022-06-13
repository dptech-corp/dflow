import os
import uuid
import shutil
import string
import random
import tarfile
import tempfile
import jsonpickle
from minio import Minio
from minio.api import CopySource
from .common import S3Artifact

s3_config = {
    "endpoint": "127.0.0.1:9000",
    "access_key": "admin",
    "secret_key": "password",
    "secure": False,
    "bucket_name": "my-bucket"
}

def download_artifact(artifact, extract=True, **kwargs):
    """
    Download an artifact from Argo to local

    Args:
        artifact: artifact to be downloaded
        extract: extract files if the artifact is compressed
        path: local path
        endpoint: endpoint for Minio
        access_key: access key for Minio
        secret_key: secret key for Minio
        secure: secure or not for Minio
        bucket_name: bucket name for Minio
    """
    if hasattr(artifact, "s3"):
        if hasattr(artifact, "archive") and hasattr(artifact.archive, "none") and artifact.archive.none is not None:
            path = download_s3(key=artifact.s3.key, recursive=True, **kwargs)
        else:
            path = download_s3(key=artifact.s3.key, recursive=False, **kwargs)
            if path[-4:] == ".tgz" and extract:
                tf = tarfile.open(path, "r:gz")
                with tempfile.TemporaryDirectory() as tmpdir:
                    tf.extractall(tmpdir)
                    tf.close()

                    os.remove(path)
                    path = os.path.dirname(path)

                    # if the artifact contains only one directory, merge the directory with the target directory
                    ld = os.listdir(tmpdir)
                    if len(ld) == 1 and os.path.isdir(os.path.join(tmpdir, ld[0])):
                        merge_dir(os.path.join(tmpdir, ld[0]), path)
                    else:
                        merge_dir(tmpdir, path)

        remove_empty_dir_tag(path)
        return assemble_path_list(path, remove=True)
    else:
        raise NotImplementedError()

def upload_artifact(path, archive="tar", **kwargs):
    """
    Upload an artifact from local to Argo

    Args:
        path: local path
        archive: compress format of the artifact, None for no compression
        endpoint: endpoint for Minio
        access_key: access key for Minio
        secret_key: secret key for Minio
        secure: secure or not for Minio
        bucket_name: bucket name for Minio
    """
    if not isinstance(path, list):
        path = [path]
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        path_list = []
        for i, p in enumerate(path):
            if p is None:
                continue
            if not os.path.exists(p):
                raise RuntimeError("File or directory %s not found" % p)
            abspath = os.path.abspath(p)
            if abspath.find(cwd) == 0:
                relpath = abspath[len(cwd)+1:]
            else:
                relpath = abspath[1:]
            target = os.path.join(tmpdir, relpath)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            os.symlink(abspath, target)
            path_list.append({"dflow_list_item": relpath, "order": i})
        with open(os.path.join(tmpdir, ".dflow.%s" % uuid.uuid4()), "w") as f:
            f.write(jsonpickle.dumps({"path_list": path_list}))

        if archive == "tar":
            os.chdir(os.path.dirname(tmpdir))
            tf = tarfile.open(os.path.basename(tmpdir) + ".tgz", "w:gz", dereference=True)
            tf.add(os.path.basename(tmpdir))
            tf.close()
            os.chdir(cwd)
            key = upload_s3(path=tmpdir + ".tgz", **kwargs)
            os.remove(tmpdir + ".tgz")
        else:
            key = upload_s3(path=tmpdir, **kwargs)

    return S3Artifact(key=key)

def copy_artifact(src, dst):
    """
    Copy an artifact to another on server side

    Args:
        src: source artifact
        dst: destination artifact
    """
    if hasattr(src, "s3"):
        src_key = src.s3.key
    elif hasattr(src, "key"):
        src_key = src.key
    else:
        raise NotImplementedError()

    if hasattr(dst, "s3"):
        dst_key = dst.s3.key
    elif hasattr(dst, "key"):
        dst_key = dst.key
    else:
        raise NotImplementedError()

    copy_s3(src_key, dst_key)
    return S3Artifact(key=dst_key)

def download_s3(key, path=None, recursive=True, endpoint=None, access_key=None, secret_key=None,
        secure=None, bucket_name=None, **kwargs):
    if endpoint is None:
        endpoint = s3_config["endpoint"]
    if access_key is None:
        access_key = s3_config["access_key"]
    if secret_key is None:
        secret_key = s3_config["secret_key"]
    if secure is None:
        secure = s3_config["secure"]
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]
    if path is None:
        path = "."
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if recursive:
        for obj in client.list_objects(bucket_name=bucket_name, prefix=key, recursive=True):
            rel_path = obj.object_name[len(key):]
            if rel_path[:1] == "/": rel_path = rel_path[1:]
            if rel_path == "":
                file_path = os.path.join(path, os.path.basename(key))
            else:
                file_path = os.path.join(path, rel_path)
            client.fget_object(bucket_name=bucket_name, object_name=obj.object_name, file_path=file_path)
    else:
        path = os.path.join(path, os.path.basename(key))
        client.fget_object(bucket_name=bucket_name, object_name=key, file_path=path)
    return path

def upload_s3(path, key=None, endpoint=None, access_key=None, secret_key=None, secure=None,
        bucket_name=None, **kwargs):
    if endpoint is None:
        endpoint = s3_config["endpoint"]
    if access_key is None:
        access_key = s3_config["access_key"]
    if secret_key is None:
        secret_key = s3_config["secret_key"]
    if secure is None:
        secure = s3_config["secure"]
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]
    if key is None:
        key = "upload/%s/%s" % (uuid.uuid4(), os.path.basename(path))
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if os.path.isfile(path):
        client.fput_object(bucket_name=bucket_name, object_name=key, file_path=path)
    elif os.path.isdir(path):
        for dn, ds, fs in os.walk(path, followlinks=True):
            rel_path = dn[len(path):]
            if rel_path == "":
                pass
            elif rel_path[0] != "/":
                rel_path = "/" + rel_path
            for f in fs:
                client.fput_object(bucket_name=bucket_name, object_name="%s%s/%s" % (key, rel_path, f), file_path=os.path.join(dn, f))
    return key

def copy_s3(src_key, dst_key, recursive=True, key=None, endpoint=None, access_key=None, secret_key=None,
        secure=None, bucket_name=None, **kwargs):
    if endpoint is None:
        endpoint = s3_config["endpoint"]
    if access_key is None:
        access_key = s3_config["access_key"]
    if secret_key is None:
        secret_key = s3_config["secret_key"]
    if secure is None:
        secure = s3_config["secure"]
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if recursive:
        for obj in client.list_objects(bucket_name=bucket_name, prefix=src_key, recursive=True):
            client.copy_object(bucket_name, dst_key + obj.object_name[len(src_key):], CopySource(bucket_name, obj.object_name))
    else:
        client.copy_object(bucket_name, dst_key, CopySource(bucket_name, src_key))

def merge_dir(src, dst):
    for f in os.listdir(src):
        src_file = os.path.join(src, f)
        dst_file = os.path.join(dst, f)
        if not os.path.exists(dst_file):
            shutil.move(src_file, dst_file)
        elif os.path.isdir(dst_file):
            if os.path.isdir(src_file):
                merge_dir(src_file, dst_file)
            else:
                shutil.rmtree(dst_file)
                shutil.move(src_file, dst_file)
        else:
            os.remove(dst_file)
            shutil.move(src_file, dst_file)

def copy_file(src, dst, func=os.link):
    os.makedirs(os.path.abspath(os.path.dirname(dst)), exist_ok=True)
    if os.path.isdir(src):
        shutil.copytree(src, dst, copy_function=func)
    elif os.path.isfile(src):
        func(src, dst)
    else:
        raise RuntimeError("File %s not found" % src)

def assemble_path_list(art_path, remove=False):
    path_list = []
    if os.path.isdir(art_path):
        dflow_list = []
        for f in os.listdir(art_path):
            if f[:6] == ".dflow":
                with open('%s/%s' % (art_path, f), 'r') as fd:
                    for item in jsonpickle.loads(fd.read())['path_list']:
                        if item not in dflow_list: dflow_list.append(item) # remove duplicate
                if remove:
                    os.remove(os.path.join(art_path, f))
        if len(dflow_list) > 0:
            path_list = list(map(lambda x: os.path.join(art_path, x) if x is not None else None, convert_dflow_list(dflow_list)))
    return path_list

def convert_dflow_list(dflow_list):
    dflow_list.sort(key=lambda x: x['order'])
    return list(map(lambda x: x['dflow_list_item'], dflow_list))

def remove_empty_dir_tag(path):
    for dn, ds, fs in os.walk(path, followlinks=True):
        if ".empty_dir" in fs:
            os.remove(os.path.join(dn, ".empty_dir"))

def randstr(l=5):
    return "".join(random.sample(string.digits + string.ascii_lowercase, l))
