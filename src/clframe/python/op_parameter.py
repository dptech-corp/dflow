#import jsonpickle
from .opio import OPIOSign, OPIO
from pathlib import Path
from typing import Iterable, Set, Union, Any, List, get_args

ParameterAllowedTypes = Union[int, str, float]

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
        super().__init__(*args, **kwargs)
        self.jsonized_key = []

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
        elif type(value) not in get_args(ParameterAllowedTypes):
            self.jsonized_key.append(key)
            self._data[key] = jsonpickle.encode(value)
        else:
            self._data[key] = value

    
