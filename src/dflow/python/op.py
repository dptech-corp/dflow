import abc
import functools
import os
from abc import ABC

from typeguard import check_type

from .opio import OPIO, Artifact, OPIOSign, Parameter


class OP(ABC):
    """
    Python class OP

    Args:
        progress_total: an int representing total progress
        progress_current: an int representing currenet progress
    """
    progress_total = 1
    progress_current = 0

    def __init__(
            self,
            *args,
            **kwargs,
    ) -> None:
        pass

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key in ["progress_total", "progress_current"]:
            with open(os.environ.get("ARGO_PROGRESS_FILE",
                                     "ARGO_PROGRESS_FILE"), "w") as f:
                f.write("%s/%s" % (self.progress_current, self.progress_total))

    @classmethod
    @abc.abstractmethod
    def get_input_sign(cls) -> OPIOSign:
        """Get the signature of the inputs
        """

    @classmethod
    @abc.abstractmethod
    def get_output_sign(cls) -> OPIOSign:
        """Get the signature of the outputs
        """

    @abc.abstractmethod
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        """Run the OP
        """
        raise NotImplementedError

    def exec_sign_check(func):
        @functools.wraps(func)
        def wrapper_exec(self, op_in):
            OP._check_signature(op_in, self.get_input_sign())
            op_out = func(self, op_in)
            OP._check_signature(op_out, self.get_output_sign())
            return op_out
        return wrapper_exec

    @staticmethod
    def _check_signature(
            opio: OPIO,
            sign: OPIOSign,
    ) -> None:
        for ii in sign.keys():
            if ii not in opio.keys():
                if isinstance(sign[ii], Parameter) and hasattr(sign[ii],
                                                               "default"):
                    opio[ii] = sign[ii].default
                else:
                    raise RuntimeError('key %s required in signature is '
                                       'not provided by the opio' % ii)
        for ii in opio.keys():
            if ii not in sign.keys():
                raise RuntimeError(
                    'key %s in OPIO is not in its signature' % ii)
            io = opio[ii]
            ss = sign[ii]
            if isinstance(ss, Artifact):
                ss = ss.type
            if isinstance(ss, Parameter):
                ss = ss.type
            # skip type checking if the variable is None
            if io is not None:
                check_type(ii, io, ss)
