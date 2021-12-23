import os
import uuid
import shutil
import tarfile
from minio import Minio
from .io import S3Artifact

def download_artifact(artifact, extract=True, **kwargs):
    if hasattr(artifact, "s3"):
        if hasattr(artifact, "archive") and hasattr(artifact.archive, "none") and artifact.archive.none is not None:
            path = download_s3(key=artifact.s3.key, recursive=True, flat_first_dir=True, **kwargs)
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
            if len(ld) == 1:
                merge_dir(os.path.join(tmpdir, ld[0]), path)
            else:
                merge_dir(tmpdir, path)
            shutil.rmtree(tmpdir)

        return path
    else:
        raise NotImplementedError()

def upload_artifact(path, **kwargs):
    assert os.path.exists(path), "File or directory %s not found" % path
    isdir = False
    if os.path.isdir(path):
        tf = tarfile.open(path + ".tgz", "w:gz")
        tf.add(path, arcname=os.path.basename(path))
        tf.close()
        path = path + ".tgz"
        isdir = True

    key = upload_s3(path=path, **kwargs)
    if isdir: os.remove(path)
    return S3Artifact(key=key)

def download_s3(key, path=None, recursive=True, flat_first_dir=True, endpoint="127.0.0.1:9000",
            access_key="admin", secret_key="password", secure=False, bucket_name="my-bucket", **kwargs):
    if path is None:
        path = "."
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if recursive:
        name_list = [obj.object_name for obj in client.list_objects(bucket_name=bucket_name, prefix=key, recursive=True)]
        if flat_first_dir:
            rel_path = name_list[0][len(key):]
            i = rel_path.find("/", 1)
            if i != -1:
                first_dir = key + rel_path[:i+1]
                if all(map(lambda x: x[:len(first_dir)] == first_dir, name_list)):
                    key = first_dir

        for obj_name in name_list:
            rel_path = obj_name[len(key):]
            if rel_path[0] == "/": rel_path = rel_path[1:]
            file_path = os.path.join(path, rel_path)
            client.fget_object(bucket_name=bucket_name, object_name=obj_name, file_path=file_path)
    else:
        path = os.path.join(path, os.path.basename(key))
        client.fget_object(bucket_name=bucket_name, object_name=key, file_path=path)
    return path

def upload_s3(path, key=None, endpoint="127.0.0.1:9000", access_key="admin", secret_key="password",
            secure=False, bucket_name="my-bucket", **kwargs):
    if key is None:
        key = "upload/%s/%s" % (uuid.uuid4(), os.path.basename(path))
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    client.fput_object(bucket_name=bucket_name, object_name=key, file_path=path)
    return key

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
