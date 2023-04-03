import os
import shutil

from dp.launching.cli import to_runner
from dp.launching.typing import BaseModel, Field
from dp.launching.typing.io import InputFilePath, OutputDirectory
from pydantic.main import ModelMetaclass

from dflow.python import OP, Artifact, BigParameter, Parameter
from dflow.python.utils import (handle_input_artifact, handle_output_artifact,
                                handle_output_parameter)


def OP_to_parser(op_cls: OP, version: str = "1.0.0"):
    ns = {"output_dir": Field(default='./outputs', description='output dir'),
          "__annotations__": {"output_dir": OutputDirectory}}
    for name, sign in op_cls.get_input_sign().items():
        if isinstance(sign, Artifact):
            ns["__annotations__"][name] = InputFilePath
            kwargs = {}
            if hasattr(sign, "description"):
                kwargs["description"] = sign.description
            if hasattr(sign, "default"):
                kwargs["default"] = sign.default
            ns[name] = Field(**kwargs)
        elif isinstance(sign, (Parameter, BigParameter)):
            ns["__annotations__"][name] = sign.type
            kwargs = {}
            if hasattr(sign, "description"):
                kwargs["description"] = sign.description
            if hasattr(sign, "default"):
                kwargs["default"] = sign.default
            ns[name] = Field(**kwargs)
        else:
            ns["__annotations__"][name] = sign
            ns[name] = Field()

    OPOptions = ModelMetaclass.__new__(ModelMetaclass, "OPOptions",
                                       (BaseModel,), ns)

    def op_runner(opts: OPOptions) -> int:
        if hasattr(op_cls, "func"):
            op = op_cls
        else:
            op = op_cls()
        op_in = {}
        for name, sign in op_cls.get_input_sign().items():
            if isinstance(sign, Artifact):
                op_in[name] = handle_input_artifact(
                    name, sign,
                    path=os.path.abspath(getattr(opts, name).get_path()))
            else:
                op_in[name] = getattr(opts, name)
        op_out = op.execute(op_in)
        os.makedirs('outputs/parameters', exist_ok=True)
        os.makedirs('outputs/artifacts', exist_ok=True)
        for name, sign in op_cls.get_output_sign().items():
            value = op_out[name]
            if isinstance(sign, Artifact):
                if os.path.isdir('outputs/artifacts/%s' % name):
                    shutil.rmtree('outputs/artifacts/%s' % name)
                handle_output_artifact(name, value, sign, data_root=".")
            else:
                handle_output_parameter(name, value, sign, data_root=".")
        return 0

    def to_parser():
        return to_runner(
            OPOptions,
            op_runner,
            version=version,
        )
    return to_parser
