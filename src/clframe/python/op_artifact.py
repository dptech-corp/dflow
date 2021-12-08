from .opio import OPIOSign, OPIO
from pathlib import Path, PurePath
from typing import Set, Union, Any, Iterable

ArtifactAllowedTypes = Union[Path, Set[Path]]
ArtifactConvertibleTypes = Union[str, Set[str], PurePath, Set[PurePath], Path, Set[Path]]

class OPArtifactSign(OPIOSign):
    def __init__(
            self,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
    
    def __setitem__(
            self,
            key : str,
            value : Any
    ) -> None:
        """Set the type hint of the key
        """
        OPIOSign.check_type_hint(value, ArtifactAllowedTypes)
        self._data[key] = value


class OPArtifact(OPIO):
    def __init__(
            self,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)

    def __getitem__(
            self,
            key : str,
    ) -> ArtifactAllowedTypes:
        return self._data[key]

    def __setitem__(
            self,
            key : str,
            value : ArtifactConvertibleTypes,
    ) -> None:
        if type(value) is str or isinstance(value, PurePath):
            self._data[key] = Path(value)
        elif value is None:
            self._data[key] = None
        else:
            self._data[key] = set([Path(ii) for ii in value])
            
        
        
