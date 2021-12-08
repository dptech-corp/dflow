import os,pathlib
from abc import ABC
from pathlib import Path
from typing import Iterable, Set, Union, Any, get_args, get_origin
from collections.abc import MutableMapping

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

    @staticmethod
    def check_type_hint(
            value : Any,
            hint : Any,
    ):
        if get_origin(value) is Union:
            check_list = [ii for ii in get_args(value)]
        else:
            check_list = [value]
        for ii in check_list:
            if not (ii in get_args(hint)):
                raise RuntimeError(f'{ii} is not an allowed OP signature. '
                                   f'hint: all allowed signatures: {" ".join([str(ii) for ii in get_args(hint)])}')


class OPIO(MutableMapping):
    """Essentially a set of Path objects
    """    
    def __init__(
            self,
            *args,
            **kwargs
    ):
        self._data = {}
        self.tmp_data = dict(*args, **kwargs)
        for kk in self.tmp_data:
            self[kk] = self.tmp_data[kk]


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

    def keys(self):
        return self._data.keys()

    
