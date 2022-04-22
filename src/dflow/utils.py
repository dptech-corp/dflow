import os
import uuid
import shutil
import tarfile
import jsonpickle
from minio import Minio
from minio.api import CopySource
from .io import S3Artifact, randstr

def download_artifact(artifact, extract=True, **kwargs):
    """
    Download an artifact from Argo to local
    :param artifact: artifact to be downloaded
    :param extract: extract files if the artifact is compressed
    :param path: local path
    :param endpoint: endpoint for Minio
    :param access_key: access key for Minio
    :param secret_key: secret key for Minio
    :param secure: secure or not for Minio
    :param bucket_name: bucket name for Minio
    :return:
    """
    if hasattr(artifact, "s3"):
        if hasattr(artifact, "archive") and hasattr(artifact.archive, "none") and artifact.archive.none is not None:
            path = download_s3(key=artifact.s3.key, recursive=True, **kwargs)
            return path

        path = download_s3(key=artifact.s3.key, recursive=False, **kwargs)
        if path[-4:] == ".tgz" and extract:
            tf = tarfile.open(path, "r:gz")
            tmpdir = os.path.join(os.path.dirname(path), "tmp-%s" % uuid.uuid4())
            tf.extractall(tmpdir)
            tf.close()

            os.remove(path)
            path = os.path.join(os.path.dirname(path))

            # if the artifact contains only one directory, merge the directory with the target directory
            ld = os.listdir(tmpdir)
            if len(ld) == 1 and os.path.isdir(os.path.join(tmpdir, ld[0])):
                merge_dir(os.path.join(tmpdir, ld[0]), path)
            else:
                merge_dir(tmpdir, path)
            shutil.rmtree(tmpdir)

            for f in os.listdir(path):
                if f[:6] == ".dflow":
                    os.remove(os.path.join(path, f))

        return path
    else:
        raise NotImplementedError()

def upload_artifact(path, archive="tar", **kwargs):
    """
    Upload an artifact from local to Argo
    :param path: local path
    :param archive: compress format of the artifact, None for no compression
    :param endpoint: endpoint for Minio
    :param access_key: access key for Minio
    :param secret_key: secret key for Minio
    :param secure: secure or not for Minio
    :param bucket_name: bucket name for Minio
    :return:
    """
    if not isinstance(path, list):
        path = [path]
    cwd = os.getcwd()
    tmpdir = "tmp-%s" % uuid.uuid4()
    os.makedirs(tmpdir)
    path_list = []
    for i, p in enumerate(path):
        if p is None:
            continue
        if not os.path.exists(p):
            shutil.rmtree(tmpdir)
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
        tf = tarfile.open(tmpdir + ".tgz", "w:gz", dereference=True)
        tf.add(tmpdir)
        tf.close()
        key = upload_s3(path=tmpdir + ".tgz", **kwargs)
        os.remove(tmpdir + ".tgz")
    else:
        key = upload_s3(path=tmpdir, **kwargs)

    shutil.rmtree(tmpdir)
    return S3Artifact(key=key)

def copy_artifact(src, dst):
    """
    Copy an artifact to another on server side
    :param src: source artifact
    :param dst: destination artifact
    :return:
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

def download_s3(key, path=None, recursive=True, endpoint="127.0.0.1:9000",
            access_key="admin", secret_key="password", secure=False, bucket_name="my-bucket", **kwargs):
    if path is None:
        path = "."
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if recursive:
        for obj in client.list_objects(bucket_name=bucket_name, prefix=key, recursive=True):
            rel_path = obj.object_name[len(key):]
            if rel_path[:1] == "/": rel_path = rel_path[1:]
            if rel_path[:6] == ".dflow": continue
            if rel_path == "":
                file_path = os.path.join(path, os.path.basename(key))
            else:
                file_path = os.path.join(path, rel_path)
            client.fget_object(bucket_name=bucket_name, object_name=obj.object_name, file_path=file_path)
    else:
        path = os.path.join(path, os.path.basename(key))
        client.fget_object(bucket_name=bucket_name, object_name=key, file_path=path)
    return path

def upload_s3(path, key=None, endpoint="127.0.0.1:9000", access_key="admin", secret_key="password",
            secure=False, bucket_name="my-bucket", **kwargs):
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

def copy_s3(src_key, dst_key, recursive=True, endpoint="127.0.0.1:9000", access_key="admin", secret_key="password",
            secure=False, bucket_name="my-bucket", **kwargs):
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
