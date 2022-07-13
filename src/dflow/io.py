import os
import tempfile
from collections import UserDict
from copy import deepcopy
from typing import Any, Dict, List, Union

import jsonpickle

from .common import S3Artifact
from .config import config
from .utils import randstr, upload_s3

try:
    from argo.workflows.client import (V1alpha1ArchiveStrategy, V1alpha1Inputs,
                                       V1alpha1Outputs, V1alpha1RawArtifact)

    from .client import V1alpha1Artifact, V1alpha1Parameter, V1alpha1ValueFrom
except Exception:
    pass

NotAllowedInputArtifactPath = ["/", "/tmp"]


class AutonamedDict(UserDict):
    def __init__(self, *args, **kwargs):
        self.step = kwargs.get("step", None)
        self.template = kwargs.get("template", None)
        super().__init__(*args)

    def __setitem__(self, key, value):
        value.name = key
        value.step = self.step
        value.template = self.template
        super().__setitem__(key, value)

    def set_step(self, step):
        self.step = step
        for value in self.values():
            value.step = step

    def set_template(self, template):
        self.template = template
        for value in self.values():
            value.template = template


class InputParameters(AutonamedDict):
    def __setitem__(self, key, value):
        assert isinstance(value, InputParameter)
        super().__setitem__(key, value)


class InputArtifacts(AutonamedDict):
    def __setitem__(self, key, value):
        assert isinstance(value, InputArtifact)
        super().__setitem__(key, value)
        if config["save_path_as_parameter"] and self.template is not None:
            if isinstance(value.source, S3Artifact):
                self.template.inputs.parameters["dflow_%s_path_list" % key] = \
                    InputParameter(value=value.source.path_list)
            else:
                self.template.inputs.parameters["dflow_%s_path_list" % key] = \
                    InputParameter(value=[])

    def set_template(self, template):
        super().set_template(template)
        if config["save_path_as_parameter"]:
            for name, art in self.items():
                if isinstance(art.source, S3Artifact):
                    self.template.inputs.parameters["dflow_%s_path_list"
                                                    % name] = InputParameter(
                        value=art.source.path_list)
                else:
                    self.template.inputs.parameters["dflow_%s_path_list"
                                                    % name] = InputParameter(
                        value=[])


class OutputParameters(AutonamedDict):
    def __setitem__(self, key, value):
        assert isinstance(value, OutputParameter)
        super().__setitem__(key, value)


class OutputArtifacts(AutonamedDict):
    def __setitem__(self, key, value):
        assert isinstance(value, OutputArtifact)
        super().__setitem__(key, value)
        if config["save_path_as_parameter"] and self.template is not None:
            self.template.outputs.parameters["dflow_%s_path_list" % key] = \
                OutputParameter(value=[])
            value.handle_path_list()

    def set_template(self, template):
        super().set_template(template)
        if config["save_path_as_parameter"]:
            for name, art in self.items():
                self.template.outputs.parameters["dflow_%s_path_list" % name]\
                    = OutputParameter(value=[])
                art.handle_path_list()


class ArgoVar:
    def __init__(self, expr=None):
        self.expr = expr

    def __repr__(self):
        return self.expr

    def __getitem__(self, i):
        if isinstance(i, str):
            return ArgoVar("jsonpath(%s, '$')['%s']" % (self.expr, i))
        else:
            return ArgoVar("jsonpath(%s, '$')[%s]" % (self.expr, i))

    def __eq__(self, other):
        if isinstance(other, ArgoVar):
            other = other.expr
        elif isinstance(other, str):
            other = "'%s'" % other
        else:
            other = "'%s'" % jsonpickle.dumps(other)
        return ArgoVar("%s == %s" % (self.expr, other))

    def __ne__(self, other):
        if isinstance(other, ArgoVar):
            other = other.expr
        elif isinstance(other, str):
            other = "'%s'" % other
        else:
            other = "'%s'" % jsonpickle.dumps(other)
        return ArgoVar("%s != %s" % (self.expr, other))

    def __lt__(self, other):
        if isinstance(other, ArgoVar):
            other = "asFloat(%s)" % other.expr
        return ArgoVar("asFloat(%s) < %s" % (self.expr, other))

    def __le__(self, other):
        if isinstance(other, ArgoVar):
            other = "asFloat(%s)" % other.expr
        return ArgoVar("asFloat(%s) <= %s" % (self.expr, other))

    def __gt__(self, other):
        if isinstance(other, ArgoVar):
            other = "asFloat(%s)" % other.expr
        return ArgoVar("asFloat(%s) > %s" % (self.expr, other))

    def __ge__(self, other):
        if isinstance(other, ArgoVar):
            other = "asFloat(%s)" % other.expr
        return ArgoVar("asFloat(%s) >= %s" % (self.expr, other))


class IfExpression:
    def __init__(
            self,
            _if: Union[str, ArgoVar],
            _then: Union[str, ArgoVar],
            _else: Union[str, ArgoVar],
    ) -> None:
        self._if = _if
        self._then = _then
        self._else = _else

    def __repr__(self) -> str:
        if isinstance(self._if, (InputParameter, OutputParameter)):
            _if = "%s == true" % self._if.expr
        elif isinstance(self._if, ArgoVar):
            _if = self._if.expr
        else:
            _if = self._if
        _then = self._then.expr if isinstance(
            self._then, ArgoVar) else self._then
        _else = self._else.expr if isinstance(
            self._else, ArgoVar) else self._else
        return "%s ? %s : %s" % (_if, _then, _else)


def if_expression(
    _if: Union[str, ArgoVar],
    _then: Union[str, ArgoVar],
    _else: Union[str, ArgoVar],
) -> IfExpression:
    """
    Return an if expression in Argo

    Args:
        _if: a bool expression, which may be a comparison of two Argo
            parameters
        _then: value returned if the condition is satisfied
        _else: value returned if the condition is not satisfied
    """
    return IfExpression(_if, _then, _else)


class PVC:
    def __init__(
            self,
            name: str,
            subpath: str,
            size: str = "1Gi",
            storage_class: str = None,
            access_modes: List[str] = None,
    ) -> None:
        self.name = name
        self.subpath = subpath
        self.size = size
        self.storage_class = storage_class
        if access_modes is None:
            access_modes = ["ReadWriteOnce"]
        self.access_modes = access_modes


class InputParameter(ArgoVar):
    """
    Input parameter for OP template

    Args:
        name: name of the input parameter
        type: parameter type
        value: default value
    """

    def __init__(
            self,
            name: str = None,
            step=None,
            template=None,
            type: Any = None,
            save_as_artifact: bool = False,
            path: str = None,
            source: Union["InputArtifact",
                          "OutputArtifact", S3Artifact] = None,
            **kwargs,
    ) -> None:
        self.name = name
        self.step = step
        self.template = template
        self.type = type
        if "value" in kwargs:
            self.value = kwargs["value"]
        self.save_as_artifact = save_as_artifact
        self.path = path
        self.source = source

    def __getattr__(self, key):
        if key == "expr":
            if self.save_as_artifact:
                if self.name is not None:
                    if self.step is not None:
                        if hasattr(self.step, "is_task"):
                            return "tasks['%s'].inputs.artifacts"\
                                "['dflow_bigpar_%s']" % (
                                    self.step.id, self.name)
                        else:
                            return "steps['%s'].inputs.artifacts["\
                                "'dflow_bigpar_%s']" % (
                                    self.step.id, self.name)
                    return "inputs.artifacts['dflow_bigpar_%s']" % self.name
                return ""
            if self.name is not None:
                if self.step is not None:
                    if hasattr(self.step, "is_task"):
                        return "tasks['%s'].inputs.parameters['%s']" % \
                            (self.step.id, self.name)
                    else:
                        return "steps['%s'].inputs.parameters['%s']" % \
                            (self.step.id, self.name)
                return "inputs.parameters['%s']" % self.name
            return ""
        return super().__getattr__(key)

    def __repr__(self):
        if self.save_as_artifact:
            if self.name is not None:
                if self.step is not None:
                    if hasattr(self.step, "is_task"):
                        return "{{tasks.%s.inputs.artifacts.dflow_bigpar_%s}}"\
                            % (self.step.id, self.name)
                    else:
                        return "{{steps.%s.inputs.artifacts.dflow_bigpar_%s}}"\
                            % (self.step.id, self.name)
                return "{{inputs.artifacts.dflow_bigpar_%s}}" % self.name
            return ""
        if self.name is not None:
            if self.step is not None:
                if hasattr(self.step, "is_task"):
                    return "{{tasks.%s.inputs.parameters.%s}}" % (self.step.id,
                                                                  self.name)
                else:
                    return "{{steps.%s.inputs.parameters.%s}}" % (self.step.id,
                                                                  self.name)
            return "{{inputs.parameters.%s}}" % self.name
        return ""

    def convert_to_argo(self):
        description = None
        if self.type is not None:
            description = jsonpickle.dumps({"type": str(self.type)})

        if self.save_as_artifact:
            if hasattr(self, "value"):
                if isinstance(self.value, (InputParameter, OutputParameter,
                                           InputArtifact, OutputArtifact)):
                    return V1alpha1Artifact(name="dflow_bigpar_" + self.name,
                                            path=self.path,
                                            _from=str(self.value))
                else:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        content = {}
                        if isinstance(self.value, str):
                            content["value"] = self.value
                        else:
                            content["value"] = jsonpickle.dumps(self.value)
                        if self.type is not None:
                            content["type"] = str(self.type)
                        path = tmpdir + "/" + self.name
                        with open(path, "w") as f:
                            f.write(jsonpickle.dumps(content))
                        key = upload_s3(path)
                        s3 = S3Artifact(key=key)
                    return V1alpha1Artifact(name="dflow_bigpar_" + self.name,
                                            path=self.path, s3=s3)
            elif isinstance(self.source, (InputParameter, OutputParameter,
                                          InputArtifact, OutputArtifact)):
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name,
                                        path=self.path, _from=str(self.source))
            else:
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name,
                                        path=self.path)

        if not hasattr(self, "value"):
            return V1alpha1Parameter(name=self.name, description=description)
        elif isinstance(self.value, ArgoVar):
            return V1alpha1Parameter(name=self.name,
                                     value="{{=%s}}" % self.value.expr,
                                     description=description)
        elif isinstance(self.value, str):
            return V1alpha1Parameter(name=self.name,
                                     value=self.value, description=description)
        else:
            return V1alpha1Parameter(name=self.name,
                                     value=jsonpickle.dumps(self.value),
                                     description=description)


class InputArtifact(ArgoVar):
    """
    Input artifact for OP template

    Args:
        path: path where the input artifact is placed in the container
        name: name of the input artifact
        optional: optional artifact or not
        type: artifact type
        source: default source
    """

    def __init__(
            self,
            path: str = None,
            name: str = None,
            step=None,
            template=None,
            optional: bool = False,
            type: Any = None,
            source: Union[str, "InputArtifact",
                          "OutputArtifact", S3Artifact] = None,
            mode: int = None,
            sub_path: str = None,
            **kwargs,
    ) -> None:
        self.path = path
        self.name = name
        self.step = step
        self.template = template
        self.optional = optional
        self.type = type
        self.source = source
        self._sub_path = None
        self.mode = mode
        self.sub_path = sub_path

    def __getattr__(self, key):
        if key == "expr":
            if self.name is not None:
                if self.step is not None:
                    if hasattr(self.step, "is_task"):
                        return "tasks['%s'].inputs.artifacts['%s']" % \
                            (self.step.id, self.name)
                    else:
                        return "steps['%s'].inputs.artifacts['%s']" % \
                            (self.step.id, self.name)
                return "inputs.artifacts['%s']" % self.name
            return ""
        return super().__getattr__(key)

    def __repr__(self):
        if self.name is not None:
            if self.step is not None:
                if hasattr(self.step, "is_task"):
                    return "{{tasks.%s.inputs.artifacts.%s}}" % (self.step.id,
                                                                 self.name)
                else:
                    return "{{steps.%s.inputs.artifacts.%s}}" % (self.step.id,
                                                                 self.name)
            return "{{inputs.artifacts.%s}}" % self.name
        return ""

    def sub_path(self, path):
        artifact = deepcopy(self)
        artifact._sub_path = path
        return artifact

    def get_path_list_parameter(self):
        return self.template.inputs.parameters["dflow_%s_path_list" %
                                               self.name]

    def convert_to_argo(self):
        if self.path in NotAllowedInputArtifactPath:
            raise RuntimeError(
                "Path [%s] is not allowed for input artifact" % self.path)
        if self.source is None:
            return V1alpha1Artifact(name=self.name, path=self.path,
                                    optional=self.optional, mode=self.mode)
        if isinstance(self.source, (InputArtifact, OutputArtifact)):
            sub_path = self.sub_path if self.sub_path is not None else \
                self.source._sub_path
            return V1alpha1Artifact(name=self.name, path=self.path,
                                    optional=self.optional,
                                    _from=str(self.source), sub_path=sub_path,
                                    mode=self.mode)
        elif isinstance(self.source, S3Artifact):
            return V1alpha1Artifact(name=self.name, path=self.path,
                                    optional=self.optional, s3=self.source,
                                    sub_path=self.sub_path, mode=self.mode)
        elif isinstance(self.source, str):
            return V1alpha1Artifact(name=self.name, path=self.path,
                                    optional=self.optional,
                                    raw=V1alpha1RawArtifact(data=self.source),
                                    mode=self.mode)
        else:
            raise RuntimeError(
                "Cannot pass an object of type %s to artifact %s" %
                (type(self.source), self))


class OutputParameter(ArgoVar):
    """
    Output parameter for OP template

    Args:
        value_from_path: the value is read from file generated in the container
        value_from_parameter: the value is from another parameter
        name: name of the output parameter
        type: parameter type
        default: default value
        global_name: global name of the parameter within the workflow
        value_from_expression: the value is from an expression
        value: specify value directly
    """

    def __init__(
            self,
            value_from_path: str = None,
            value_from_parameter: Union[InputParameter,
                                        "OutputParameter"] = None,
            name: str = None,
            step=None,
            template=None,
            type: Any = None,
            global_name: str = None,
            value_from_expression: Union[str, IfExpression] = None,
            save_as_artifact: bool = False,
            **kwargs,
    ) -> None:
        self.value_from_path = value_from_path
        self.value_from_parameter = value_from_parameter
        self.name = name
        self.step = step
        self.template = template
        self.type = type
        self.global_name = global_name
        self.value_from_expression = value_from_expression
        self.save_as_artifact = save_as_artifact
        if "default" in kwargs:
            self.default = kwargs["default"]
        if "value" in kwargs:
            self.value = kwargs["value"]

    def __getattr__(self, key):
        if key == "expr":
            if self.save_as_artifact:
                if self.name is not None:
                    if self.step is not None:
                        if hasattr(self.step, "is_task"):
                            return "tasks['%s'].outputs.artifacts"\
                                "['dflow_bigpar_%s']" % (
                                    self.step.id, self.name)
                        else:
                            return "steps['%s'].outputs.artifacts"\
                                "['dflow_bigpar_%s']" % (
                                    self.step.id, self.name)
                    return "outputs.artifacts['dflow_bigpar_%s']" % self.name
            if self.name is not None:
                if self.step is not None:
                    if hasattr(self.step, "is_task"):
                        return "tasks['%s'].outputs.parameters['%s']" % \
                            (self.step.id, self.name)
                    else:
                        return "steps['%s'].outputs.parameters['%s']" % \
                            (self.step.id, self.name)
                return "outputs.parameters['%s']" % self.name
            return ""
        return super().__getattr__(key)

    def __setattr__(self, key, value):
        if key == "value_from_parameter" and \
                isinstance(value, (InputParameter, OutputParameter)):
            if self.save_as_artifact:
                value.save_as_artifact = True
            if value.save_as_artifact:
                self.save_as_artifact = True
            if self.type is None and value.type is not None:
                self.type = value.type
            if value.type is None and self.type is not None:
                value.type = self.type
        return super().__setattr__(key, value)

    def __repr__(self):
        if self.save_as_artifact:
            if self.name is not None:
                if self.step is not None:
                    if hasattr(self.step, "is_task"):
                        return \
                            "{{tasks.%s.outputs.artifacts.dflow_bigpar_%s}}" \
                            % (self.step.id, self.name)
                    else:
                        return \
                            "{{steps.%s.outputs.artifacts.dflow_bigpar_%s}}" \
                            % (self.step.id, self.name)
                return "{{outputs.artifacts.dflow_bigpar_%s}}" % self.name
        if self.name is not None:
            if self.step is not None:
                if hasattr(self.step, "is_task"):
                    return "{{tasks.%s.outputs.parameters.%s}}" % (
                        self.step.id, self.name)
                else:
                    return "{{steps.%s.outputs.parameters.%s}}" % (
                        self.step.id, self.name)
            return "{{outputs.parameters.%s}}" % self.name
        return ""

    def convert_to_argo(self):
        description = None
        if self.type is not None:
            description = jsonpickle.dumps({"type": str(self.type)})

        if self.save_as_artifact:
            if self.value_from_path is not None:
                return V1alpha1Artifact(
                    name="dflow_bigpar_" + self.name,
                    path=self.value_from_path,
                    archive=V1alpha1ArchiveStrategy(_none={}),
                    global_name=self.global_name)
            elif self.value_from_parameter is not None:
                return V1alpha1Artifact(
                    name="dflow_bigpar_" + self.name,
                    _from=str(self.value_from_parameter),
                    archive=V1alpha1ArchiveStrategy(_none={}),
                    global_name=self.global_name)
            elif self.value_from_expression is not None:
                return V1alpha1Artifact(
                    name="dflow_bigpar_" + self.name,
                    from_expression=str(self.value_from_expression),
                    archive=V1alpha1ArchiveStrategy(_none={}),
                    global_name=self.global_name)
            else:
                raise RuntimeError("Not supported.")

        default = None
        if hasattr(self, "default"):
            default = self.default if isinstance(
                self.default, str) else jsonpickle.dumps(self.default)

        if self.value_from_path is not None:
            return V1alpha1Parameter(
                name=self.name,
                value_from=V1alpha1ValueFrom(
                    path=self.value_from_path,
                    default=default),
                global_name=self.global_name,
                description=description)
        elif self.value_from_parameter is not None:
            return V1alpha1Parameter(
                name=self.name,
                value_from=V1alpha1ValueFrom(
                    parameter=str(self.value_from_parameter),
                    default=default),
                global_name=self.global_name,
                description=description)
        elif self.value_from_expression is not None:
            return V1alpha1Parameter(
                name=self.name,
                value_from=V1alpha1ValueFrom(
                    expression=str(self.value_from_expression),
                    default=default),
                global_name=self.global_name,
                description=description)
        elif hasattr(self, "value"):
            value = self.value if isinstance(
                self.value, str) else jsonpickle.dumps(self.value)
            return V1alpha1Parameter(name=self.name, value=value,
                                     global_name=self.global_name,
                                     description=description)
        else:
            raise RuntimeError("Output parameter %s is not specified" % self)


class OutputArtifact(ArgoVar):
    """
    Output artifact for OP template

    Args:
        path: path of the output artifact in the container
        _from: the artifact is from another artifact
        name: name of the output artifact
        type: artifact type
        save: place to store the output artifact instead of default storage,
            can be a list
        archive: compress format of the artifact, None for no compression
        global_name: global name of the artifact within the workflow
        from_expression: the artifact is from an expression
    """

    def __init__(
            self,
            path: os.PathLike = None,
            _from: Union[InputArtifact, "OutputArtifact"] = None,
            name: str = None,
            step=None,
            template=None,
            type: Any = None,
            save: List[Union[PVC, S3Artifact]] = None,
            archive: str = "default",
            global_name: str = None,
            from_expression: Union[IfExpression, str] = None,
            **kwargs,
    ) -> None:
        self.path = path
        self.name = name
        self.step = step
        self.template = template
        self.type = type
        if save is None:
            save = []
        elif not isinstance(save, list):
            save = [save]
        self.save = save
        if archive == "default":
            archive = config["archive_mode"]
        self.archive = archive
        self._sub_path = None
        self.global_name = global_name
        self.from_expression = from_expression
        self.redirect = None
        self._from = _from

    def sub_path(self, path):
        artifact = deepcopy(self)
        artifact._sub_path = path
        return artifact

    def __getattr__(self, key):
        if key == "expr":
            if self.redirect is not None:
                return self.redirect.expr
            if self.global_name is not None:
                return "workflow.outputs.artifacts['%s']" % (self.global_name)
            elif self.name is not None:
                if self.step is not None:
                    if hasattr(self.step, "is_task"):
                        return "tasks['%s'].outputs.artifacts['%s']" % (
                            self.step.id, self.name)
                    else:
                        return "steps['%s'].outputs.artifacts['%s']" % (
                            self.step.id, self.name)
                return "outputs.artifacts['%s']" % self.name
            return ""
        return super().__getattr__(key)

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if config["save_path_as_parameter"] and key in ["_from",
                                                        "from_expression"]:
            self.handle_path_list()

    def handle_path_list(self):
        if self.template is not None:
            if hasattr(self, "_from") and self._from is not None:
                self.template.outputs.parameters["dflow_%s_path_list" %
                                                 self.name].default = "."
                self.template.outputs.parameters[
                    "dflow_%s_path_list" % self.name].value_from_parameter = \
                    self._from.get_path_list_parameter()
            elif hasattr(self, "from_expression") and self.from_expression is \
                    not None:
                self.template.outputs.parameters["dflow_%s_path_list" %
                                                 self.name].default = "."
                self.template.outputs.parameters[
                    "dflow_%s_path_list" % self.name].value_from_expression = \
                    if_expression(
                    _if=self.from_expression._if,
                    _then=self.from_expression._then.get_path_list_parameter(),
                    _else=self.from_expression._else.get_path_list_parameter(),
                )

    def get_path_list_parameter(self):
        return self.step.outputs.parameters["dflow_%s_path_list" % self.name]

    def __repr__(self):
        if self.redirect is not None:
            return str(self.redirect)
        if self.global_name is not None:
            return "{{workflow.outputs.artifacts.%s}}" % (self.global_name)
        elif self.name is not None:
            if self.step is not None:
                if hasattr(self.step, "is_task"):
                    return "{{tasks.%s.outputs.artifacts.%s}}" % (self.step.id,
                                                                  self.name)
                else:
                    return "{{steps.%s.outputs.artifacts.%s}}" % (self.step.id,
                                                                  self.name)
            return "{{outputs.artifacts.%s}}" % self.name
        return ""

    def pvc(self, size="1Gi", storage_class=None, access_modes=None):
        pvc = PVC("public", randstr(), size, storage_class, access_modes)
        self.save.append(pvc)
        return pvc

    def convert_to_argo(self):
        if self.archive is None:
            archive = V1alpha1ArchiveStrategy(_none={})
        elif self.archive == "tar":
            archive = None
        else:
            raise RuntimeError("Archive type %s not supported" % self.archive)

        s3 = None
        for save in self.save:
            if isinstance(save, S3Artifact):
                s3 = save

        if self.path is not None:
            return V1alpha1Artifact(name=self.name, path=self.path,
                                    archive=archive, s3=s3,
                                    global_name=self.global_name)
        elif self._from is not None:
            return V1alpha1Artifact(name=self.name, _from=str(self._from),
                                    archive=archive, s3=s3,
                                    global_name=self.global_name)
        elif self.from_expression is not None:
            return V1alpha1Artifact(
                name=self.name, from_expression=str(self.from_expression),
                archive=archive, s3=s3, global_name=self.global_name)
        else:
            raise RuntimeError("Output artifact %s is not specified" % self)


class Inputs:
    """
    Inputs for OP template

    Args:
        parameters: input parameters
        artifacts: input artifacts
    """

    def __init__(
            self,
            parameters: Dict[str, InputParameter] = None,
            artifacts: Dict[str, InputArtifact] = None,
            step=None,
            template=None,
    ) -> None:
        self.step = step
        self.template = template
        if parameters is not None:
            self.parameters = parameters
        else:
            self.parameters = InputParameters(
                step=self.step, template=self.template)

        if artifacts is not None:
            self.artifacts = artifacts
        else:
            self.artifacts = InputArtifacts(
                step=self.step, template=self.template)

    def __setattr__(self, key, value):
        if key == "parameters":
            assert isinstance(value, (dict, UserDict))
            super().__setattr__(key, InputParameters(
                value, step=self.step, template=self.template))
        elif key == "artifacts":
            assert isinstance(value, (dict, UserDict))
            super().__setattr__(key, InputArtifacts(
                value, step=self.step, template=self.template))
        else:
            super().__setattr__(key, value)

    def set_step(self, step):
        self.step = step
        self.parameters.set_step(step)
        self.artifacts.set_step(step)

    def set_template(self, template):
        self.template = template
        self.parameters.set_template(template)
        self.artifacts.set_template(template)

    def convert_to_argo(self):
        parameters = []
        artifacts = []
        for par in self.parameters.values():
            if par.save_as_artifact:
                artifacts.append(par.convert_to_argo())
            else:
                parameters.append(par.convert_to_argo())
        for art in self.artifacts.values():
            artifacts.append(art.convert_to_argo())
        return V1alpha1Inputs(parameters=parameters, artifacts=artifacts)


class Outputs:
    """
    Outputs for OP template

    Args:
        paramters: output parameters
        artifacts: output artifacts
    """

    def __init__(
            self,
            parameters: Dict[str, OutputParameter] = None,
            artifacts: Dict[str, OutputArtifact] = None,
            step=None,
            template=None,
    ) -> None:
        self.step = step
        self.template = template
        if parameters is not None:
            self.parameters = parameters
        else:
            self.parameters = OutputParameters(
                step=self.step, template=self.template)

        if artifacts is not None:
            self.artifacts = artifacts
        else:
            self.artifacts = OutputArtifacts(
                step=self.step, template=self.template)

    def __setattr__(self, key, value):
        if key == "parameters":
            assert isinstance(value, (dict, UserDict))
            super().__setattr__(key, OutputParameters(
                value, step=self.step, template=self.template))
        elif key == "artifacts":
            assert isinstance(value, (dict, UserDict))
            super().__setattr__(key, OutputArtifacts(
                value, step=self.step, template=self.template))
        else:
            super().__setattr__(key, value)

    def set_step(self, step):
        self.step = step
        self.parameters.set_step(step)
        self.artifacts.set_step(step)

    def set_template(self, template):
        self.template = template
        self.parameters.set_template(template)
        self.artifacts.set_template(template)

    def convert_to_argo(self):
        parameters = []
        artifacts = []
        for par in self.parameters.values():
            if par.save_as_artifact:
                artifacts.append(par.convert_to_argo())
            else:
                parameters.append(par.convert_to_argo())
        for art in self.artifacts.values():
            artifacts.append(art.convert_to_argo())
        return V1alpha1Outputs(parameters=parameters, artifacts=artifacts)
