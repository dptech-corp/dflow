import inspect
import os
import shutil
from typing import Union

from dp.launching.cli import to_runner
from dp.launching.typing import BaseModel, Field
from dp.launching.typing.io import InputFilePath, OutputDirectory
from pydantic.main import ModelMetaclass

from dflow import (OPTemplate, S3Artifact, Step, Workflow, download_artifact,
                   upload_s3)
from dflow.python import OP, Artifact, BigParameter, Parameter
from dflow.python.utils import (handle_input_artifact, handle_output_artifact,
                                handle_output_parameter)


def OP_template_to_parser(templ: OPTemplate, version: str = "1.0.0"):
    ns = {"output_dir": Field(default='./outputs', description='output dir'),
          "__annotations__": {"output_dir": OutputDirectory}}
    for name, par in templ.inputs.parameters.items():
        ns["__annotations__"][name] = par.type
        kwargs = {}
        if hasattr(par, "description"):
            kwargs["description"] = par.description
        if hasattr(par, "value"):
            kwargs["default"] = par.value
        ns[name] = Field(**kwargs)
    for name, art in templ.inputs.artifacts.items():
        ns["__annotations__"][name] = InputFilePath
        kwargs = {}
        if hasattr(art, "description"):
            kwargs["description"] = art.description
        ns[name] = Field(**kwargs)

    OPOptions = ModelMetaclass.__new__(ModelMetaclass, "OPOptions",
                                       (BaseModel,), ns)

    def op_runner(opts: OPOptions) -> int:
        step = Step("dflow-main", templ, parameters={
            name: getattr(opts, name)
            for name in templ.inputs.parameters.keys()
        }, artifacts={
            name: S3Artifact(key=upload_s3(getattr(opts, name).get_path()))
            for name in templ.inputs.artifacts.keys()
        })
        wf = Workflow(templ.name)
        wf.add(step)
        wf.submit()
        wf.wait()
        assert wf.query_status() == "Succeeded"
        step = wf.query_step("dflow-main")[0]
        if hasattr(step, "outputs") and hasattr(step.outputs, "parameters"):
            os.makedirs('outputs/parameters', exist_ok=True)
            for name, par in step.outputs.parameters.items():
                with open('outputs/parameters/%s' % name, 'w') as f:
                    f.write(par["value"])
        if hasattr(step, "outputs") and hasattr(step.outputs, "artifacts"):
            for name, art in step.outputs.artifacts.items():
                download_artifact(art, path='outputs/artifact/%s' % name,
                                  remove_catalog=False)
        return 0

    def to_parser():
        return to_runner(
            OPOptions,
            op_runner,
            version=version,
        )
    return to_parser


def python_OP_to_parser(op: OP, version: str = "1.0.0"):
    ns = {"output_dir": Field(default='./outputs', description='output dir'),
          "__annotations__": {"output_dir": OutputDirectory}}
    for name, sign in op.get_input_sign().items():
        if isinstance(sign, Artifact):
            ns["__annotations__"][name] = InputFilePath
            kwargs = {}
            if hasattr(sign, "description"):
                kwargs["description"] = sign.description
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
        op_in = {}
        for name, sign in op.get_input_sign().items():
            if isinstance(sign, Artifact):
                op_in[name] = handle_input_artifact(
                    name, sign,
                    path=os.path.abspath(getattr(opts, name).get_path()))
            else:
                op_in[name] = getattr(opts, name)
        op_out = op.execute(op_in)
        os.makedirs('outputs/parameters', exist_ok=True)
        os.makedirs('outputs/artifacts', exist_ok=True)
        for name, sign in op.get_output_sign().items():
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


def OP_to_parser(op: Union[OP, OPTemplate], version: str = "1.0.0"):
    if inspect.isclass(op) and issubclass(op, OP):
        return python_OP_to_parser(op(), version)
    elif isinstance(op, OP):
        return python_OP_to_parser(op, version)
    elif isinstance(op, OPTemplate):
        return OP_template_to_parser(op, version)
    else:
        raise ValueError("Only Python OP/OP template supported")
