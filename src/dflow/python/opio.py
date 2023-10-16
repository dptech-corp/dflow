import json
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import jsonpickle

from ..common import CustomHandler, S3Artifact
from ..config import config
from ..io import PVC, type_to_str


class nested_dict:
    def __init__(self, type):
        self.type = type

    def __repr__(self):
        return "dflow.python.NestedDict[%s]" % type_to_str(self.type)

    def __eq__(self, other):
        if not isinstance(other, nested_dict):
            return False
        return self.type == other.type


NestedDict = {
    str: nested_dict(str),
    Path: nested_dict(Path),
}

ArtifactAllowedTypes = [str, Path, Set[str], Set[Path], List[str], List[Path],
                        Dict[str, str], Dict[str, Path], NestedDict[str],
                        NestedDict[Path]]


@CustomHandler.handles
class Artifact:
    """
    OPIO signature of artifact

    Args:
        type: str, Path, Set[str], Set[Path], List[str], List[Path],
            Dict[str, str], Dict[str, Path], NestedDict[str] or
            NestedDict[Path]
        archive: compress format of the artifact, None for no compression
        save: place to store the output artifact instead of default storage,
            can be a list
        optional: optional input artifact or not
        global_name: global name of the artifact within the workflow
    """

    def __init__(
            self,
            type: Any,
            archive: str = "default",
            save: List[Union[PVC, S3Artifact]] = None,
            optional: bool = False,
            global_name: Optional[str] = None,
            sub_path: bool = True,
            **kwargs,
    ) -> None:
        self.type = type
        if archive == "default":
            archive = config["archive_mode"]
        self.archive = archive
        self.save = save
        self.optional = optional
        self.global_name = global_name
        self.sub_path = sub_path
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def __setattr__(self, key, value):
        if key == "type":
            assert (value in ArtifactAllowedTypes), "%s is not allowed" \
                                                    "artifact type, only %s " \
                                                    "are allowed." % (
                                                        value,
                                                        ArtifactAllowedTypes)
        super().__setattr__(key, value)

    def to_dict(self):
        return {
            "type": type_to_str(self.type),
            "archive": self.archive,
            "save": self.save,
            "optional": self.optional,
            "global_name": self.global_name,
            "sub_path": self.sub_path,
        }

    @classmethod
    def from_dict(cls, d):
        if isinstance(d["type"], str):
            d["type"] = {type_to_str(t): t for t in ArtifactAllowedTypes}[
                d["type"]]
        return cls(**d)

    def __repr__(self):
        return "Artifact(type=%s, optional=%s, sub_path=%s)" % (
            type_to_str(self.type), self.optional, self.sub_path)

    def __call__(self, *args, **kwargs):
        self.type(*args, **kwargs)


@CustomHandler.handles
class Parameter:
    """
    OPIO signature of parameter

    Args:
        type: parameter type
        global_name: global name of the parameter within the workflow
        default: default value of the parameter
    """

    def __init__(
            self,
            type: Any,
            global_name: Optional[str] = None,
            **kwargs,
    ) -> None:
        self.type = type
        self.global_name = global_name
        if "default" in kwargs:
            self.default = kwargs["default"]
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def __repr__(self):
        default = ""
        if hasattr(self, "default"):
            try:
                default = ", default=%s" % json.dumps(self.default)
            except Exception:
                default = ", default=jsonpickle.loads('%s')" % \
                    jsonpickle.dumps(self.default)
        return "Parameter(type=%s%s)" % (type_to_str(self.type), default)

    def to_dict(self):
        d = {
            "type": type_to_str(self.type),
            "global_name": self.global_name,
        }
        if hasattr(self, "default"):
            d["default"] = self.default
        return d

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


@CustomHandler.handles
class BigParameter:
    """
    OPIO signature of big parameter

    Args:
        type: parameter type
    """

    def __init__(
            self,
            type: Any,
            **kwargs,
    ) -> None:
        self.type = type
        if "default" in kwargs:
            self.default = kwargs["default"]
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def __repr__(self):
        default = ""
        if hasattr(self, "default"):
            try:
                default = ", default=%s" % json.dumps(self.default)
            except Exception:
                default = ", default=jsonpickle.loads('%s')" % \
                    jsonpickle.dumps(self.default)
        return "BigParameter(type=%s%s)" % (type_to_str(self.type), default)

    def to_dict(self):
        d = {
            "type": type_to_str(self.type),
        }
        if hasattr(self, "default"):
            d["default"] = self.default
        return d

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


class OPIOSign(MutableMapping):
    """The signature of OPIO.
    A signature of OPIO includes the key and its typing
    """

    def __init__(
            self,
            *args,
            **kwargs
    ):
        self._data = {}
        self._data = dict(*args, **kwargs)

    def __getitem__(
            self,
            key: str,
    ) -> Any:
        """Get the type hint of the key
        """
        return self._data[key]

    def __setitem__(
            self,
            key: str,
            value: Any,
    ) -> None:
        """Set the type hint of the key
        """
        self._data[key] = value

    def __delitem__(
            self,
            key: str,
    ) -> None:
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return str(self._data)


class OPIO(MutableMapping):
    def __init__(
            self,
            *args,
            **kwargs
    ):
        self._data = {}
        self._data = dict(*args, **kwargs)

    def __getitem__(
            self,
            key: str,
    ) -> Any:
        return self._data[key]

    def __setitem__(
            self,
            key: str,
            value: Any,
    ) -> None:
        self._data[key] = value

    def __delitem__(
            self,
            key: str,
    ) -> None:
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return str(self._data)
