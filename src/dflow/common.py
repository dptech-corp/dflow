import abc
import logging
import os
import shutil
from abc import ABC
from copy import copy, deepcopy
from typing import Any, Dict, List, Union

from .config import config as global_config
from .config import s3_config

try:
    from argo.workflows.client import V1alpha1OSSArtifact, V1alpha1S3Artifact
    from argo.workflows.client.configuration import Configuration
except Exception:
    V1alpha1S3Artifact = object


class S3Artifact(V1alpha1S3Artifact):
    """
    S3 artifact

    Args:
        key: key of the s3 artifact
    """

    def __init__(
            self,
            path_list: Union[str, list] = None,
            urn: str = "",
            *args,
            **kwargs,
    ) -> None:
        config = Configuration()
        config.client_side_validation = False
        super().__init__(local_vars_configuration=config, *args, **kwargs)
        if urn:
            if global_config["lineage"]:
                meta = global_config["lineage"].get_artifact_metadata(urn)
                self.key = meta.uri
            else:
                logging.warn("Lineage client not provided")
        assert isinstance(self.key, str)
        if not self.key.startswith(s3_config["prefix"]) and not any(
                [self.key.startswith(p) for p in s3_config["extra_prefixes"]]):
            self.key = s3_config["prefix"] + self.key
        if path_list is None:
            path_list = []
        self.path_list = path_list
        self.urn = urn
        self.slice = None
        self.parent = None

    def __getitem__(self, key):
        art = copy(self)
        art.parent = self
        if art.slice is None:
            art.slice = key
        else:
            art.slice = "%s.%s" % (art.slice, key)
        return art

    def to_dict(self):
        d = {"key": self.key, "urn": self.urn}
        if s3_config["storage_client"] is None:
            d.update(s3_config)
        else:
            d.update(s3_config["storage_client"].to_dict())
        return d

    @classmethod
    def from_dict(cls, d):
        return cls(key=d["key"], urn=d.get("urn", ""))

    def sub_path(
            self,
            path: str,
    ) -> Any:
        artifact = deepcopy(self)
        if artifact.key[-1:] != "/":
            artifact.key += "/"
        artifact.key += path
        return artifact

    def download(self, **kwargs):
        from .utils import download_artifact
        download_artifact(self, **kwargs)

    def oss(self):
        config = Configuration()
        config.client_side_validation = False
        return V1alpha1OSSArtifact(key=s3_config["repo_prefix"] + self.key,
                                   local_vars_configuration=config)


class LocalArtifact:
    def __init__(self, local_path):
        self.local_path = local_path
        self.slice = None
        self.parent = None

    def __getitem__(self, key):
        art = copy(self)
        art.parent = self
        if art.slice is None:
            art.slice = key
        else:
            art.slice = "%s.%s" % (art.slice, key)
        return art


class HTTPArtifact:
    def __init__(self, url):
        self.url = url

    def download(self, path="."):
        os.makedirs(path, exist_ok=True)
        file_path = os.path.join(path, os.path.basename(self.url))
        import requests
        sess = requests.session()
        with sess.get(self.url, stream=True, verify=False) as req:
            req.raise_for_status()
            with open(file_path, 'w') as f:
                shutil.copyfileobj(req.raw, f.buffer)
        return file_path


class LineageClient(ABC):
    @abc.abstractmethod
    def register_workflow(
            self,
            workflow_name: str) -> str:
        pass

    @abc.abstractmethod
    def register_artifact(
            self,
            namespace: str,
            name: str,
            uri: str,
            **kwargs) -> str:
        pass

    @abc.abstractmethod
    def register_task(
            self,
            task_name: str,
            input_urns: Dict[str, Union[str, List[str]]],
            output_uris: Dict[str, str],
            workflow_urn: str) -> Dict[str, str]:
        pass

    @abc.abstractmethod
    def get_artifact_metadata(self, urn: str) -> object:
        pass


class CustomArtifact(ABC):
    @abc.abstractmethod
    def get_urn(self) -> str:
        pass

    def __repr__(self):
        return self.get_urn()

    @abc.abstractmethod
    def download(self, name: str, path: str):
        pass
