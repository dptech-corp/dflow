from pathlib import Path
from typing import Union, Any, Set, List
from collections.abc import MutableMapping

ArtifactAllowedTypes = [str, Path, Set[str], Set[Path], List[str], List[Path]]

class Artifact:
    def __init__(self, type, archive="tar", save=None, optional=False, slices=None):
        self.type = type
        self.archive = archive
        self.save = save
        self.optional = optional
        self.slices = slices

    def __setattr__(self, key, value):
        if key == "type":
            assert (value in ArtifactAllowedTypes), "%s is not allowed artifact type, only %s are allowed." % (value, ArtifactAllowedTypes)
        super().__setattr__(key, value)

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
            key : str,
    ) -> Any:
        """Get the type hint of the key
        """
        return self._data[key]

    def __setitem__(
            self,
            key : str,
            value : Any,
    ) -> None:
        """Set the type hint of the key
        """
        self._data[key] = value

    def __delitem__(
            self,
            key : str,
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
            key : str,
    ) -> Any:
        return self._data[key]

    def __setitem__(
            self,
            key : str,
            value : Any,
    ) -> None:
        self._data[key] = value

    def __delitem__(
            self,
            key : str,
    ) -> None:
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return str(self._data)
