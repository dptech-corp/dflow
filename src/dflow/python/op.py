import abc
import functools
import inspect
import os
import pathlib
import warnings
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
    subclass = {}

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
                elif isinstance(sign[ii], Artifact) and sign[ii].optional:
                    opio[ii] = None
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

    @classmethod
    def function(cls, func=None, *, msg=None):
        if func is None:
            return functools.partial(cls.function, msg=msg)

        signature = func.__annotations__
        return_type = signature.get('return', None)

        def get_input_sign():
            return OPIOSign(
                {k: v for k, v in signature.items() if k != 'return'})

        class subclass(cls, abc.ABC):
            pass

        cls.subclass[func.__name__] = subclass
        cls.subclass[func.__name__].get_input_sign = get_input_sign

        def get_output_sign():
            if isinstance(return_type, dict):
                return OPIOSign({k: v for k, v in return_type.items()})
            elif return_type.hasattr('__annotations__'):
                return OPIOSign(
                    {k: v for k, v in return_type.__annotations__.items()})
            elif not return_type:
                warnings.warn(
                    'We recommended using return type signature like:'
                    '\n'
                    "def func()->TypedDict('op',{'x': int, 'y': str})")
                return {}
            else:
                raise ValueError(
                    'Unknown return value annotation, '
                    f'Expected class dict or typing.TypedDict, '
                    f'got {type(return_type)}.')

        cls.subclass[func.__name__].get_output_sign = get_output_sign

        def execute(
                self,
                op_in: OPIO,
        ) -> OPIO:
            op_out = func(**op_in)
            return op_out

        cls.subclass[func.__name__].execute = execute
        cls.subclass[func.__name__].func = func
        cls.subclass[func.__name__].__name__ = func.__name__
        input_sign_dict = {k: v for k, v in signature.items() if
                           k != 'return'}
        input_sign = '{'
        # type of value defined in annotation maybe builtin type or Artifact;
        # Artifact has type name from typing package(List), or builtin
        # type(str) and also pathlib.Path
        for k, v in input_sign_dict.items():
            if type(v) == type:
                input_sign += f'"{k}":{v.__name__},'
            else:
                input_sign += f'"{k}":{f"Artifact({v.type_string})"}'
        input_sign += '}'
        output_sign = "{"
        if isinstance(return_type, dict):
            output_sign_dict = {k: v for k, v in return_type.items()}
        elif return_type.hasattr('__annotations__'):
            output_sign_dict = {k: v for k, v in
                                return_type.__annotations__.items()}
        elif not return_type:
            warnings.warn(
                'We recommended using return type signature like:'
                '\n'
                "def func()->TypedDict('op',{'x': int, 'y': str})")
            output_sign_dict = {}
        else:
            raise ValueError(
                'Unknown return value annotation, '
                f'Expected class dict or typing.TypedDict, '
                f'got {type(return_type)}.')
        for k, v in output_sign_dict.items():
            if type(v) == type:
                output_sign += f'"{k}":{v.__name__}'
            else:
                output_sign += f'"{k}":{f"Artifact({v.type_string})"}'
        output_sign += '}'
        sourcelinnum = inspect.getsourcelines(func)[1]
        cls.subclass[func.__name__].script = "\n".join(
            pathlib.Path(
                inspect.getsourcefile(func)
            ).read_text().split("\n")[:sourcelinnum - 1])
        cls.subclass[func.__name__].script += \
            """from dflow.python import (Artifact, OPIOSign) \n""" + \
            """import typing\nimport pathlib\n""" + \
            f"""class {func.__name__}(OP):           \n""" + \
            """    @classmethod                     \n""" + \
            """    def get_input_sign(cls):         \n""" + \
            """        return OPIOSign(             \n""" + \
            """                {})                  \n""".format(
                input_sign
            ) + \
            """                                     \n""" + \
            """    @classmethod                     \n""" + \
            """    def get_output_sign(cls):        \n""" + \
            """        return OPIOSign({})          \n""".format(
                output_sign) + \
            """    @OP.exec_sign_check              \n""" + \
            """    def execute(self,op_in: OPIO,):   \n""" + \
            """        {}                           \n""".format(
                "\n        ".join(inspect.getsource(func).split("\n")[1:])) + \
            f"""        return {func.__name__}(**op_in)\n""" + \
            """"""
        return cls.subclass[func.__name__]
