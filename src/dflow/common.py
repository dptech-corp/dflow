import abc
import logging
import os
import re
import shutil
from abc import ABC
from copy import copy, deepcopy
from importlib import import_module
from typing import Any, Dict, List, Union

import jsonpickle

from .config import config as global_config
from .config import s3_config

try:
    from argo.workflows.client import V1alpha1OSSArtifact, V1alpha1S3Artifact
    from argo.workflows.client.configuration import Configuration
except Exception:
    V1alpha1S3Artifact = object

field_regex = re.compile("^[a-zA-Z0-9][-a-zA-Z0-9]*$")
field_errmsg = "name must consist of alpha-numeric characters or '-', and "\
    "must start with an alpha-numeric character (e.g. My-name1-2, 123-NAME)"
subdomain_regex = re.compile(
    "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")
subdomain_errmsg = "a lowercase RFC 1123 subdomain must consist of lower case"\
    " alpha-numeric characters, '-' or '.', and must start and end with an "\
    "alpha-numeric character (e.g. 'example.com')"
param_regex = re.compile("^[-a-zA-Z0-9_]+[-a-zA-Z0-9_]*$")
param_errmsg = "parameter/artifact name must consist of alpha-numeric "\
    "characters, '_' or '-' e.g. my_param_1, MY-PARAM-1"
key_regex = re.compile("^[a-z0-9][-a-z0-9]*$")
key_errmsg = "key must consist of lower case alpha-numeric characters or '-',"\
    "and must start with an alpha-numeric character (e.g. 'my-key')"
input_parameter_pattern = re.compile(r"^{{inputs\.parameters\.(.*)}}$")
input_artifact_pattern = re.compile(r"^{{inputs\.artifacts\.(.*)}}$")
step_output_parameter_pattern = re.compile(
    r"^{{steps\.(.*?)\.outputs\.parameters\.(.*?)}}$")
step_output_artifact_pattern = re.compile(
    r"^{{steps\.(.*?)\.outputs\.artifacts\.(.*?)}}$")
task_output_parameter_pattern = re.compile(
    r"^{{tasks\.(.*?)\.outputs\.parameters\.(.*?)}}$")
task_output_artifact_pattern = re.compile(
    r"^{{tasks\.(.*?)\.outputs\.artifacts\.(.*?)}}$")


class CustomHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data.update(obj.to_dict())
        return data

    def restore(self, obj):
        cls = import_func(obj.pop("py/object"))
        return cls.from_dict(obj)


@CustomHandler.handles
class S3Artifact(V1alpha1S3Artifact):
    """
    S3 artifact

    Args:
        key: key of the s3 artifact
    """

    def __init__(
            self,
            key: str = None,
            path_list: Union[str, list] = None,
            urn: str = "",
            debug_s3: bool = False,
            *args,
            **kwargs,
    ) -> None:
        config = Configuration()
        config.client_side_validation = False
        super().__init__(local_vars_configuration=config, key=key, *args,
                         **kwargs)
        if urn:
            if global_config["lineage"]:
                meta = global_config["lineage"].get_artifact_metadata(urn)
                self.key = meta.uri
            else:
                logging.warning("Lineage client not provided")
        assert isinstance(self.key, str)
        self.local_path = None
        if global_config["mode"] == "debug" and not global_config["debug_s3"] \
                and not debug_s3:
            self.local_path = self.key
        elif not self.key.startswith(s3_config["prefix"]) and not any(
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
        d = {"key": self.key, "urn": self.urn, "slice": self.slice}
        if s3_config["storage_client"] is None:
            d.update(s3_config)
        else:
            d.update(s3_config["storage_client"].to_dict())
        return d

    @classmethod
    def from_dict(cls, d):
        artifact = cls(key=d["key"], urn=d.get("urn", ""))
        artifact.slice = d.get("slice")
        return artifact

    def sub_path(
            self,
            path: str,
    ) -> Any:
        artifact = deepcopy(self)
        if artifact.key[-1:] != "/":
            artifact.key += "/"
        artifact.key += str(path)
        return artifact

    def download(self, **kwargs):
        from .utils import download_artifact
        download_artifact(self, **kwargs)

    def oss(self):
        config = Configuration()
        config.client_side_validation = False
        return V1alpha1OSSArtifact(key=s3_config["repo_prefix"] + self.key,
                                   local_vars_configuration=config)

    def evalable_repr(self, imports):
        args = "key='%s'" % self.key
        if self.urn:
            args += ", urn='%s'" % self.urn
        imports.add(("dflow", "S3Artifact"))
        return "S3Artifact(%s)" % args


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

    def sub_path(
            self,
            path: str,
    ) -> Any:
        artifact = deepcopy(self)
        artifact.local_path += "/%s" % path
        return artifact


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


def import_func(s):
    fields = s.split(".")
    if fields[0] == __name__ or fields[0] == "":
        fields[0] = ""
        mod = import_module(".".join(fields[:-1]), package=__name__)
    else:
        mod = import_module(".".join(fields[:-1]))
    return getattr(mod, fields[-1])


class CustomArtifact(ABC):
    redirect = None

    @abc.abstractmethod
    def get_urn(self) -> str:
        pass

    @staticmethod
    def from_urn(urn):
        protocol = urn.split("://")[0]
        name = global_config["artifact_register"][protocol]
        custom = import_func(name)
        return custom.from_urn(urn)

    def __repr__(self):
        return self.get_urn()

    @abc.abstractmethod
    def download(self, name: str, path: str):
        pass

    def render(self, template, name: str):
        return template
