import abc,os,functools
from typing import List,Set,Tuple
from abc import ABC
from enum import Enum
from pathlib import Path
from typeguard import check_type
from .opio import OPIO, OPIOSign
#from .context import Context
from .op_parameter import OPParameterSign, OPParameter
from .op_artifact import OPArtifactSign, OPArtifact

class Status(object):
    """The status of DP-GEN events
    """
    INITED = 0
    EXECUTED = 1
    ERROR = 2

class OP(ABC):
    """The OP of DP-GEN. The OP is defined as an operation that has some
    effects on the files in the system. One can get all the files
    needed for the OP by `get_input` and all files output by
    `get_output`. The action of the OP is activated by `execute`.

    """
    def __init__(
            self,
            context,
    )->None:
        self._context = context

    @property
    def context(self):
        return self._context
    
    @property
    def status(self):
        return self._status
    
    @property
    def work_path (self):
        return self._work_path

    @classmethod
    @abc.abstractmethod
    def get_input_parameter_sign(cls) -> OPParameterSign:
        """Get the signiture of the inputs
        """
    
    @classmethod
    @abc.abstractmethod
    def get_output_parameter_sign(cls) -> OPParameterSign:
        """Get the signiture of the outputs
        """
    
    @classmethod
    @abc.abstractmethod
    def get_input_artifact_sign(cls) -> OPArtifactSign:
        """Get the signiture of the inputs
        """
    
    @classmethod
    @abc.abstractmethod
    def get_output_artifact_sign(cls) -> OPArtifactSign:
        """Get the signiture of the outputs
        """
    
    @abc.abstractmethod
    def execute (
            self,
            op_parameter : OPParameter,
            op_artifact : OPArtifact,
    ) -> Tuple[OPParameter, OPArtifact]:
        """Run the OP
        """
        raise NotImplementedError

    def set_status(status):
        def decorator_set_status(func):
            @functools.wraps(func)
            def wrapper_set_status(self, *args, **kwargs):
                ret = func(self, *args, **kwargs)
                self._status = status
                return ret
            return wrapper_set_status
        return decorator_set_status

    def exec_sign_check(func):
        @functools.wraps(func)
        def wrapper_exec(self, i_p, i_a):
            OP._check_signature(i_p, self.get_input_parameter_sign())
            OP._check_signature(i_a, self.get_input_artifact_sign())
            (o_p, o_a) = func(self, i_p, i_a)
            OP._check_signature(o_p, self.get_output_parameter_sign())
            OP._check_signature(o_a, self.get_output_artifact_sign())
            return (o_p, o_a)
        return wrapper_exec

    @staticmethod
    def _check_signature(
            opio : OPIO,
            sign : OPIOSign,
    ) -> None:
        for ii in sign.keys() :
            if ii not in opio.keys():
                raise RuntimeError(f'key {ii} required in signature is not provided by the opio')
        for ii in opio.keys() :
            if ii not in sign.keys():
                raise RuntimeError(f'key {ii} in OPIO is not in its signature')
            io = opio[ii]
            ss = sign[ii]
            # skip type checking if the variable is None
            if io is not None:
                check_type(ii, io, ss)        


    @staticmethod
    def _backup_path(path):
        if path.is_dir() : 
            dirname = path.name
            counter = 0
            while True :
                bk_dirname = Path(dirname + ".bk%03d" % counter)
                if not bk_dirname.is_dir():
                    path.replace(path.parent / bk_dirname)
                    break
                counter += 1
            (path.parent / dirname).mkdir(parents=True)

    @staticmethod
    def create_path (
            path : Path,
            exists_ok : bool = False,
    ) -> None :
        """Create path. 

        Parameters
        ----------
        path
                The path to be created
        exists_ok
                If True, then do nothing if path exists
                Otherwise if path exists, it will be backuped to path.bk%03d.    
        """
        if path.is_dir() :
            if exists_ok :
                return
            else :
                OP._backup_path(path)
        path.mkdir(parents=True)

