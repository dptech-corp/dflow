import os
import re

from .dispatcher import BohriumArtifact


class DatasetsArtifact(BohriumArtifact):
    def __init__(self, element, version, type="datasets"):
        self.element = element
        self.version = version
        self.type = type

    @classmethod
    def from_urn(cls, urn: str):
        matched = re.compile(r'^launching\+(?P<type>datasets|models)://'
                             '(?P<element>.*?)(@(?P<version>.*))?$').match(urn)
        assert matched, "Invalid URN: %s" % urn
        element = matched.group("element")
        version = matched.group("version")
        type = matched.group("type")
        return cls(element, version, type)

    def get_urn(self) -> str:
        return "launching+%s://%s@%s" % (self.type, self.element, self.version)

    def get_bohrium_urn(self, name: str) -> str:
        mount_type = "rw" if self.version == "draft" else "ro"
        return "launching://%s/%s@%s?alias=%s&action=%s" % (
            self.type, self.element, self.version, name, mount_type)

    def download(self, name: str, path: str):
        cmd = "rclone mount %s@%s %s" % (self.element, self.version, path)
        ret = os.system(cmd)
        assert ret == 0, "Command %s failed" % cmd

    def bohrium_download(self, name: str, path: str):
        os.symlink("/launching/%s" % name, path)
