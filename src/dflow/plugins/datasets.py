import os
import re
import subprocess
import time
from copy import deepcopy

from ..config import config as global_config
from ..python import PythonOPTemplate
from .dispatcher import DispatcherArtifact

try:
    from argo.workflows.client import (V1alpha1UserContainer,
                                       V1EmptyDirVolumeSource,
                                       V1HostPathVolumeSource,
                                       V1SecurityContext, V1Volume,
                                       V1VolumeMount)
except Exception:
    pass

global_config["artifact_register"]["launching+datasets"] = \
    "dflow.plugins.datasets.DatasetsArtifact"
global_config["artifact_register"]["launching+models"] = \
    "dflow.plugins.datasets.DatasetsArtifact"


def wait_for_mount(point, timeout=60):
    for _ in range(timeout):
        df_info = os.popen("df -h").read()
        for line in df_info.splitlines()[1:]:
            if line.split()[-1] == point:
                return
        time.sleep(1)
    raise TimeoutError("Time out waiting for mounting on %s" % point)


config = {
    "rclone_image": os.environ.get("RCLONE_IMAGE", "rclone/rclone:1.62.2"),
    "rclone_image_pull_policy": os.environ.get("RCLONE_IMAGE_PULL_POLICY"),
    "rclone_type": os.environ.get("DATASETS_RCLONE_TYPE", "ftp"),
    "ftp_host": os.environ.get("DATASETS_FTP_HOST",
                               "uftp.mlops-passthrough.dp.tech"),
    "webdav_host": os.environ.get("DATASETS_WEBDAV_HOST",
                                  "https://webdav.mlops.dp.tech"),
    "user": os.environ.get("DATASETS_USER"),
    "password": os.environ.get("DATASETS_PASSWORD"),
}


class DatasetsArtifact(DispatcherArtifact):
    def __init__(self, element, version, type="datasets",
                 rclone_image=None, rclone_image_pull_policy=None,
                 rclone_type=None, ftp_host=None, webdav_host=None,
                 user=None, password=None, rclone_kwargs=None, sub_path=None):
        self.element = element
        self.version = version
        self.type = type
        if rclone_image is None:
            rclone_image = config["rclone_image"]
        if rclone_image_pull_policy is None:
            rclone_image_pull_policy = config["rclone_image_pull_policy"]
        self.rclone_image = rclone_image
        self.rclone_image_pull_policy = rclone_image_pull_policy
        if rclone_type is None:
            rclone_type = config["rclone_type"]
        self.rclone_type = rclone_type
        if ftp_host is None:
            ftp_host = config["ftp_host"]
        if webdav_host is None:
            webdav_host = config["webdav_host"]
        if user is None:
            user = config["user"]
        if password is None:
            password = config["password"]
        if self.rclone_type == "ftp":
            self.rclone_kwargs = {
                "host": ftp_host,
                "user": user,
                "pass": password,
                "explicit_tls": True,
                "disable_tls13": True,
                "concurrency": 3,
            }
        elif self.rclone_type == "webdav":
            self.rclone_kwargs = {
                "url": "%s/%s~%s@%s" % (webdav_host, type, element, version),
                "user": user,
                "pass": password,
                "vendor": "other",
                "concurrency": 100,
            }
        if rclone_kwargs is not None:
            self.rclone_kwargs.update(rclone_kwargs)
        self._sub_path = sub_path

    @classmethod
    def from_urn(cls, urn: str):
        matched = re.compile(r'^launching\+(?P<type>datasets|models)://'
                             '(?P<element>.*?)(@(?P<version>.*))?$').match(urn)
        assert matched, "Invalid URN: %s" % urn
        element = matched.group("element")
        version = matched.group("version")
        type = matched.group("type")
        i = version.find("/")
        if i != -1:
            sub_path = version[i+1:]
            version = version[:i]
        else:
            sub_path = None
        return cls(element, version, type, sub_path=sub_path)

    @classmethod
    def from_rclone_config(cls, config: str):
        lines = list(filter(lambda x: x.strip(), config.splitlines()))
        matched = re.compile(
            r'\[(?P<element>.*?)(@(?P<version>.*))?\]$').match(lines[0])
        assert matched, "Invalid RClone config: %s" % config
        element = matched.group("element")
        version = matched.group("version")
        rclone_kwargs = {}
        for line in lines[1:]:
            fields = line.split("=")
            assert len(fields) == 2, "Invalid RClone key-value pair: %s" % line
            key = fields[0].strip()
            value = fields[1].strip()
            if key == "type":
                rclone_type = value
            else:
                rclone_kwargs[key] = value
        return cls(element, version, rclone_type=rclone_type,
                   rclone_kwargs=rclone_kwargs)

    def sub_path(self, path: str):
        artifact = deepcopy(self)
        if artifact._sub_path is None:
            artifact._sub_path = str(path)
        else:
            artifact._sub_path += "/%s" % path
        return artifact

    def get_urn(self) -> str:
        if self._sub_path is not None:
            return "launching+%s://%s@%s/%s" % (
                self.type, self.element, self.version, self._sub_path)
        else:
            return "launching+%s://%s@%s" % (
                self.type, self.element, self.version)

    def get_bohrium_urn(self, name: str) -> str:
        mount_type = "rw" if self.version == "draft" else "ro"
        return "launching://%s/%s@%s?alias=%s&action=%s" % (
            self.type, self.element, self.version, name, mount_type)

    def modify_config(self, name: str, machine) -> str:
        if "job_resources" not in machine.input_data:
            machine.input_data["job_resources"] = []
        machine.input_data["job_resources"].append(self.get_bohrium_urn(name))

    def download(self, name: str, path: str):
        wait_for_mount("/launching/%s" % name)
        if self._sub_path is not None:
            os.symlink("/launching/%s/%s" % (name, self._sub_path), path)
        else:
            os.symlink("/launching/%s" % name, path)

    def remote_download(self, name: str, path: str):
        cmd = self.get_mount_script("/launching/%s" % name)
        p = subprocess.Popen(cmd, shell=True, start_new_session=True)
        wait_for_mount("/launching/%s" % name)
        if self._sub_path is not None:
            os.symlink("/launching/%s/%s" % (name, self._sub_path), path)
        else:
            os.symlink("/launching/%s" % name, path)
        return p.pid

    def bohrium_download(self, name: str, path: str):
        os.stat("/launching/%s" % name)
        if self._sub_path is not None:
            os.symlink("/launching/%s/%s" % (name, self._sub_path), path)
        else:
            os.symlink("/launching/%s" % name, path)

    def get_mount_script(self, path):
        script = "rclone config create %s@%s %s %s" % (
            self.element, self.version, self.rclone_type,
            " ".join([k + " " + (str(v).lower() if isinstance(
                v, bool) else str(v)) for k, v in self.rclone_kwargs.items()]))
        script += " && mkdir -p %s" % path
        script += " && rclone mount %s@%s: %s" % (
            self.element, self.version, path)
        return script

    def render(self, template: PythonOPTemplate, name: str
               ) -> PythonOPTemplate:
        if "launching" not in [v.name for v in template.volumes]:
            template.volumes.append(V1Volume(
                name="launching",
                empty_dir=V1EmptyDirVolumeSource()))
        if "dev-fuse" not in [v.name for v in template.volumes]:
            template.volumes.append(V1Volume(
                name="dev-fuse",
                host_path=V1HostPathVolumeSource(path="/dev/fuse")))
        if "launching" not in [m.name for m in template.mounts]:
            template.mounts.append(V1VolumeMount(
                name="launching",
                mount_path="/launching",
                mount_propagation="HostToContainer"))
        script = "rclone config create %s@%s %s %s" % (
            self.element, self.version, self.rclone_type,
            " ".join([k + " " + (str(v).lower() if isinstance(
                v, bool) else str(v)) for k, v in self.rclone_kwargs.items()]))
        script += " && mkdir -p /launching/%s" % name
        script += " && rclone mount %s@%s: /launching/%s" % (
            self.element, self.version, name)
        template.sidecars.append(V1alpha1UserContainer(
            name="rclone-%s" % name.replace("_", "-"),
            image=self.rclone_image,
            image_pull_policy=self.rclone_image_pull_policy,
            command=["sh", "-c"],
            args=[self.get_mount_script("/launching/%s" % name)],
            security_context=V1SecurityContext(privileged=True),
            volume_mounts=[
                V1VolumeMount(name="launching", mount_path="/launching",
                              mount_propagation="Bidirectional"),
                V1VolumeMount(name="dev-fuse", mount_path="/dev/fuse"),
            ],
        ))
        template.render_script()
        return template
