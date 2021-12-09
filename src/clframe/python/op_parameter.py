import jsonpickle
from .opio import OPIOSign, OPIO
from pathlib import Path
from typing import Iterable, Set, Union, Any, List, get_args
from typeguard import check_type

ParameterAllowedTypes = Union[int, str, float, bool, List[int], List[str], List[float], List[bool]]

class OPParameterSign(OPIOSign):
    def __init__(
            self,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
    
    def __setitem__(
            self,
            key : str,
            value : Any,
    ) -> None:
        """Set the type hint of the key
        """
        OPIOSign.check_type_hint(value, ParameterAllowedTypes)
        self._data[key] = value    


class OPParameter(OPIO):
    def __init__(
            self,
            *args,
            **kwargs,
    ):
        self.jsonized_key = []
        super().__init__(*args, **kwargs)

    def __getitem__(
            self,
            key : str,
    ) -> ParameterAllowedTypes:
        # if key in self.jsonized_key:
        #     return jsonpickle.decode(self._data[key])
        # else:
        #     return self._data[key]
        return self._data[key]

    def __setitem__(
            self,
            key : str,
            value : Any,
    ) -> None:
        # if the value is not an allowed type
        # jsonize the value
        if value is None:
            self._data[key] = None
        elif self._not_allowed_type(value):
            self.jsonized_key.append(key)
            self._data[key] = jsonpickle.encode(value)
        else:
            self._data[key] = value

    def is_jsonized(
            self,
            key : str,
    )->bool:
        """
        Check if the value of the corresponding key is jsonized.
        If yes, one may use jsonpickle.decode to recover the value.

        Parameters
        ----------
        key : str
                The key

        Returns
        -------
        is_jsonized: bool
                Boolean indicating if the value is jsonized
        """
        return key in self.jsonized_key

    def _not_allowed_type(
            self,
            value,
    )->bool:
        record = []
        for idx,tt in enumerate(get_args(ParameterAllowedTypes)):
            try :
                check_type(None, value, tt)
            except TypeError:
                record.append(False)
            else:
                record.append(True)
        return not any(record)
