import abc
import base64
import functools
import inspect
import json
import os
import warnings
from abc import ABC
from pathlib import Path
from typing import Dict

from typeguard import check_type

from ..argo_objects import ArgoObjectDict
from ..utils import get_key, s3_config
from .opio import (OPIO, Artifact, BigParameter, OPIOSign, Parameter,
                   type_to_str)


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

    def _get_s3_link(self, key):
        if key[-4:] != ".tgz":
            key += "/"
        encoded_key = base64.b64encode(key.encode()).decode()
        return "%s/buckets/%s/browse/%s" % (
            s3_config["console"], s3_config["bucket_name"], encoded_key)

    def get_input_artifact_storage_key(self, name: str) -> str:
        templ = json.loads(os.environ.get("ARGO_TEMPLATE"))
        art = next(filter(lambda x: x["name"] == name,
                          templ["inputs"]["artifacts"]))
        return get_key(ArgoObjectDict(art))

    def get_input_artifact_link(self, name: str) -> str:
        key = self.get_input_artifact_storage_key(name)
        return self._get_s3_link(key)

    def get_output_artifact_storage_key(self, name: str) -> str:
        templ = json.loads(os.environ.get("ARGO_TEMPLATE"))
        art = next(filter(lambda x: x["name"] == name,
                          templ["outputs"]["artifacts"]))
        key = get_key(ArgoObjectDict(art), raise_error=False)
        if key is not None:
            return key

        key = get_key(ArgoObjectDict(templ["archiveLocation"]))
        if "archive" in art and "none" in art["archive"]:
            return "%s/%s" % (key, name)
        else:
            return "%s/%s.tgz" % (key, name)

    def get_output_artifact_link(self, name: str) -> str:
        key = self.get_output_artifact_storage_key(name)
        return self._get_s3_link(key)

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
            OP._check_signature(op_in, self.get_input_sign(), True)
            op_out = func(self, op_in)
            OP._check_signature(op_out, self.get_output_sign(), False)
            return op_out

        return wrapper_exec

    @staticmethod
    def _check_signature(
            opio: OPIO,
            sign: OPIOSign,
            is_input: bool,
    ) -> None:
        for ii in sign.keys():
            if ii not in opio.keys():
                if isinstance(sign[ii], Parameter) and hasattr(sign[ii],
                                                               "default"):
                    opio[ii] = sign[ii].default
                elif isinstance(sign[ii], BigParameter) and hasattr(sign[ii],"default"):
                    opio[ii] = sign[ii].default
                elif isinstance(sign[ii], Artifact) and sign[ii].optional:
                    opio[ii] = None
                else:
                    if is_input:
                        raise RuntimeError('key %s declared in the input sign'
                                           ' is not present in the input'
                                           ' passed to the OP' % ii)
                    else:
                        raise RuntimeError('key %s declared in the output sign'
                                           ' is not present in the output'
                                           ' given by the OP' % ii)
        for ii in opio.keys():
            if ii not in sign.keys():
                if is_input:
                    raise RuntimeError(
                        'key %s in the input passed to the OP is not declared'
                        ' in its input sign' % ii)
                else:
                    raise RuntimeError(
                        'key %s in the output given by the OP is not declared'
                        ' in its output sign' % ii)
            io = opio[ii]
            ss = sign[ii]
            if isinstance(ss, Artifact):
                ss = ss.type
                if ss in [Dict[str, str], Dict[str, Path]]:
                    ss = dict
            if isinstance(ss, Parameter):
                ss = ss.type
            # skip type checking if the variable is None
            if io is not None:
                check_type(ii, io, ss)

    @classmethod
    def function(cls, func):
        signature = func.__annotations__
        return_type = signature.get('return', None)

        input_sign = OPIOSign(
                {k: v for k, v in signature.items() if k != 'return'})

        if isinstance(return_type, dict):
            output_sign = OPIOSign({k: v for k, v in return_type.items()})
        elif return_type.hasattr('__annotations__'):
            output_sign = OPIOSign(
                {k: v for k, v in return_type.__annotations__.items()})
        elif not return_type:
            warnings.warn(
                'We recommended using return type signature like:'
                '\n'
                "def func()->TypedDict('op', {'x': int, 'y': str})")
            output_sign = {}
        else:
            raise ValueError(
                'Unknown return value annotation, '
                f'Expected class dict or typing.TypedDict, '
                f'got {type(return_type)}.')

        class subclass(cls):
            @classmethod
            def get_input_sign(cls):
                return input_sign

            @classmethod
            def get_output_sign(cls):
                return output_sign

            @OP.exec_sign_check
            def execute(self, op_in):
                op_out = func(**op_in)
                return op_out

            def __call__(self, **op_in):
                return self.execute(op_in)

        subclass.func = func
        subclass.__name__ = func.__name__
        subclass.__module__ = func.__module__
        return subclass()

    @classmethod
    def get_opio_info(cls, opio_sign):
        opio = {}
        for io, sign in opio_sign.items():
            if type(sign) in [Artifact, Parameter, BigParameter]:
                opio[io] = sign.to_str()
            else:
                opio[io] = type_to_str(sign)
        return opio

    @classmethod
    def get_info(cls):
        res = {}
        name = "%s.%s" % (cls.__module__, cls.__name__)
        res["name"] = name
        res["inputs"] = cls.get_opio_info(cls.get_input_sign())
        res["outputs"] = cls.get_opio_info(cls.get_output_sign())
        if hasattr(cls, "func"):
            res["execute"] = "".join(inspect.getsourcelines(cls.func)[0])
        else:
            res["execute"] = "".join(inspect.getsourcelines(cls.execute)[0])
        return res
