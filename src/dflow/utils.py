import os
import uuid
from minio import Minio
from argo.workflows.client.configuration import Configuration
from argo.workflows.client import V1alpha1S3Artifact

def download_artifact(artifact, **kwargs):
    if hasattr(artifact, "s3"):
        return download_s3(key=artifact.s3.key, **kwargs)
    else:
        raise NotImplementedError()

def upload_artifact(path, **kwargs):
    key = upload_s3(path=path, **kwargs)
    config = Configuration()
    config.client_side_validation = False
    return V1alpha1S3Artifact(key=key, local_vars_configuration=config)

def download_s3(key, path=None, endpoint="127.0.0.1:9000", access_key="admin", secret_key="password",
            secure=False, bucket_name="my-bucket", **kwargs):
    if path is None:
        path = os.path.basename(key)
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    client.fget_object(bucket_name=bucket_name, object_name=key, file_path=path)
    return path

def upload_s3(path, key=None, endpoint="127.0.0.1:9000", access_key="admin", secret_key="password",
            secure=False, bucket_name="my-bucket", **kwargs):
    if key is None:
        key = "upload/%s" % uuid.uuid4()
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    client.fput_object(bucket_name=bucket_name, object_name=key, file_path=path)
    return key
