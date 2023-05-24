import os
from typing import Optional

import oss2

from ..config import s3_config
from ..utils import StorageClient


class OSSClient(StorageClient):
    def __init__(
            self,
            endpoint: Optional[str] = None,
            bucket_name: Optional[str] = None,
            access_key_id: Optional[str] = None,
            access_key_secret: Optional[str] = None,
    ) -> None:
        if endpoint is None:
            endpoint = os.environ.get("OSS_ENDPOINT")
        if bucket_name is None:
            bucket_name = os.environ.get("OSS_BUCKET_NAME")
        if access_key_id is None:
            access_key_id = os.environ.get("OSS_ACCESS_KEY_ID")
        if access_key_secret is None:
            access_key_secret = os.environ.get("OSS_ACCESS_KEY_SECRET")
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        auth = oss2.Auth(access_key_id, access_key_secret)
        bucket = oss2.Bucket(auth, endpoint, bucket_name)
        self.bucket = bucket

    def to_dict(self):
        retained_keys = ["endpoint", "bucket_name"]
        return {k: self.__dict__[k] for k in retained_keys}

    def prefixing(self, key):
        if not key.startswith(s3_config["repo_prefix"]):
            return s3_config["repo_prefix"] + key
        return key

    def unprefixing(self, key):
        if key.startswith(s3_config["repo_prefix"]):
            return key[len(s3_config["repo_prefix"]):]
        return key

    def upload(self, key, path):
        self.bucket.put_object_from_file(self.prefixing(key), path)

    def download(self, key, path):
        if os.path.dirname(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        self.bucket.get_object_to_file(self.prefixing(key), path)

    def list(self, prefix, recursive=False):
        prefix = self.prefixing(prefix)
        keys = []
        if recursive:
            marker = ""
            while True:
                r = self.bucket.list_objects(prefix, marker=marker)
                for obj in r.object_list:
                    if not obj.key.endswith("/"):
                        keys.append(self.unprefixing(obj.key))
                if not r.is_truncated:
                    break
                marker = r.next_marker
        else:
            marker = ""
            while True:
                r = self.bucket.list_objects(prefix, delimiter="/",
                                             marker=marker)
                for obj in r.object_list:
                    if obj.key == prefix and obj.key.endswith("/"):
                        continue
                    keys.append(self.unprefixing(obj.key))
                for key in r.prefix_list:
                    keys.append(self.unprefixing(key))
                if not r.is_truncated:
                    break
                marker = r.next_marker
        return keys

    def copy(self, src, dst):
        self.bucket.copy_object(self.bucket_name, self.prefixing(src),
                                self.prefixing(dst))

    def get_md5(self, key):
        return self.bucket.get_object_meta(self.prefixing(key).etag)
