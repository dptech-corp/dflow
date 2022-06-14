import tempfile
from copy import deepcopy
from collections import UserDict
import jsonpickle
from argo.workflows.client import (
    V1alpha1Inputs,
    V1alpha1Outputs,
    V1alpha1RawArtifact,
    V1alpha1ArchiveStrategy
)
from .client import V1alpha1ValueFrom, V1alpha1Parameter, V1alpha1Artifact
from .common import S3Artifact
from .utils import upload_s3, randstr

NotAllowedInputArtifactPath = ["/", "/tmp"]

class AutonamedDict(UserDict):
    def __setitem__(self, key, value):
        value = deepcopy(value)
        value.name = key
        super().__setitem__(key, value)

class Inputs:
    """
    Inputs for OP template

    Args:
        paramters: input parameters
        artifacts: input artifacts
    """
    def __init__(self, parameters=None, artifacts=None):
        if parameters is not None:
            self.parameters = parameters
        else:
            self.parameters = AutonamedDict()
        
        if artifacts is not None:
            self.artifacts = artifacts
        else:
            self.artifacts = AutonamedDict()

    def __setattr__(self, key, value):
        if key in ["parameters", "artifacts"] and isinstance(value, dict):
            super().__setattr__(key, AutonamedDict(value))
        else:
            super().__setattr__(key, value)

    def set_step_id(self, step_id):
        for par in self.parameters.values():
            par.step_id = step_id
        for art in self.artifacts.values():
            art.step_id = step_id

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
    def __init__(self, parameters=None, artifacts=None):
        if parameters is not None:
            self.parameters = parameters
        else:
            self.parameters = AutonamedDict()
        
        if artifacts is not None:
            self.artifacts = artifacts
        else:
            self.artifacts = AutonamedDict()

    def __setattr__(self, key, value):
        if key in ["parameters", "artifacts"] and isinstance(value, dict):
            super().__setattr__(key, AutonamedDict(value))
        else:
            super().__setattr__(key, value)

    def set_step_id(self, step_id):
        for par in self.parameters.values():
            par.step_id = step_id
        for art in self.artifacts.values():
            art.step_id = step_id

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

class ArgoVar:
    def __init__(self, expr=None):
        self.expr = expr

    def __repr__(self):
        return self.expr

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

class InputParameter(ArgoVar):
    """
    Input parameter for OP template

    Args:
        name: name of the input parameter
        step_id:
        type: parameter type
        value: default value
    """
    def __init__(self, name=None, step_id=None, type=None, value=None, save_as_artifact=False, path=None, source=None):
        self.name = name
        self.step_id = step_id
        self.type = type
        self.value = value
        self.save_as_artifact = save_as_artifact
        self.path = path
        self.source = source

    def __getattr__(self, key):
        if key == "expr":
            if self.save_as_artifact:
                if self.name is not None:
                    if self.step_id is not None:
                        return "steps['%s'].inputs.artifacts['dflow_bigpar_%s']" % (self.step_id, self.name)
                    return "inputs.artifacts['dflow_bigpar_%s']" % self.name
                return ""
            if self.name is not None:
                if self.step_id is not None:
                    return "steps['%s'].inputs.parameters['%s']" % (self.step_id, self.name)
                return "inputs.parameters['%s']" % self.name
            return ""
        return super().__getattr__(key)

    def __repr__(self):
        if self.save_as_artifact:
            if self.name is not None:
                if self.step_id is not None:
                    return "{{steps.%s.inputs.artifacts.dflow_bigpar_%s}}" % (self.step_id, self.name)
                return "{{inputs.artifacts.dflow_bigpar_%s}}" % self.name
            return ""
        if self.name is not None:
            if self.step_id is not None:
                return "{{steps.%s.inputs.parameters.%s}}" % (self.step_id, self.name)
            return "{{inputs.parameters.%s}}" % self.name
        return ""

    def convert_to_argo(self):
        description = None
        if self.type is not None:
            description = jsonpickle.dumps({"type": str(self.type)})

        if self.save_as_artifact:
            if self.value is not None:
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
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name, path=self.path, s3=s3)
            elif isinstance(self.source, (InputParameter, OutputParameter)):
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name, path=self.path, _from=str(self.source))
            elif isinstance(self.source, (InputArtifact, OutputArtifact)):
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name, path=self.path, _from=str(self.source))
            else:
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name, path=self.path)

        if self.value is None:
            return V1alpha1Parameter(name=self.name, description=description)
        elif isinstance(self.value, ArgoVar):
            return V1alpha1Parameter(name=self.name, value="{{=%s}}" % self.value.expr, description=description)
        elif isinstance(self.value, str):
            return V1alpha1Parameter(name=self.name, value=self.value, description=description)
        else:
            return V1alpha1Parameter(name=self.name, value=jsonpickle.dumps(self.value), description=description)

class InputArtifact(ArgoVar):
    """
    Input artifact for OP template

    Args:
        path: path where the input artifact is placed in the container
        name: name of the input artifact
        step_id:
        optional: optional artifact or not
        type: artifact type
        source: default source
    """
    def __init__(self, path=None, name=None, step_id=None, optional=False, type=None, source=None):
        self.path = path
        self.name = name
        self.step_id = step_id
        self.optional = optional
        self.type = type
        self.source = source
        self._sub_path = None

    def __getattr__(self, key):
        if key == "expr":
            if self.name is not None:
                if self.step_id is not None:
                    return "steps['%s'].inputs.artifacts['%s']" % (self.step_id, self.name)
                return "inputs.artifacts['%s']" % self.name
            return ""
        return super().__getattr__(key)

    def __repr__(self):
        if self.name is not None:
            if self.step_id is not None:
                return "{{steps.%s.inputs.artifacts.%s}}" % (self.step_id, self.name)
            return "{{inputs.artifacts.%s}}" % self.name
        return ""

    def sub_path(self, path):
        artifact = deepcopy(self)
        artifact._sub_path = path
        return artifact

    def convert_to_argo(self):
        if self.path in NotAllowedInputArtifactPath:
            raise RuntimeError("Path [%s] is not allowed for input artifact" % self.path)
        if self.source is None:
            return V1alpha1Artifact(name=self.name, path=self.path, optional=self.optional)
        if isinstance(self.source, (InputArtifact, OutputArtifact)):
            return V1alpha1Artifact(name=self.name, path=self.path, optional=self.optional, _from=str(self.source), sub_path=self.source._sub_path)
        elif isinstance(self.source, S3Artifact):
            return V1alpha1Artifact(name=self.name, path=self.path, optional=self.optional, s3=self.source, sub_path=self.source._sub_path)
        elif isinstance(self.source, str):
            return V1alpha1Artifact(name=self.name, path=self.path, optional=self.optional, raw=V1alpha1RawArtifact(data=self.source))
        else:
            raise RuntimeError("Cannot pass an object of type %s to artifact %s" % (type(self.source)), self)

class OutputParameter(ArgoVar):
    """
    Output parameter for OP template

    Args:
        value_from_path: the value is read from file generated in the container
        value_from_parameter: the value is from another parameter
        name: name of the output parameter
        step_id:
        type: parameter type
        default: default value
        global_name: global name of the parameter within the workflow
        value_from_expression: the value is from an expression
    """
    def __init__(self, value_from_path=None, value_from_parameter=None, name=None, step_id=None, type=None, default=None, global_name=None,
            value_from_expression=None, save_as_artifact=False):
        self.value_from_path = value_from_path
        self.value_from_parameter = value_from_parameter
        self.name = name
        self.step_id = step_id
        self.type = type
        self.default = default
        self.global_name = global_name
        self.value_from_expression = value_from_expression
        self.save_as_artifact = save_as_artifact

    def __getattr__(self, key):
        if key == "expr":
            if self.save_as_artifact:
                if self.name is not None:
                    if self.step_id is not None:
                        return "steps['%s'].outputs.artifacts['dflow_bigpar_%s']" % (self.step_id, self.name)
                    return "outputs.artifacts['dflow_bigpar_%s']" % self.name
            if self.name is not None:
                if self.step_id is not None:
                    return "steps['%s'].outputs.parameters['%s']" % (self.step_id, self.name)
                return "outputs.parameters['%s']" % self.name
            return ""
        return super().__getattr__(key)

    def __setattr__(self, key, value):
        if key == "value_from_parameter" and isinstance(value, (InputParameter, OutputParameter)):
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
                if self.step_id is not None:
                    return "{{steps.%s.outputs.artifacts.dflow_bigpar_%s}}" % (self.step_id, self.name)
                return "{{outputs.artifacts.dflow_bigpar_%s}}" % self.name
        if self.name is not None:
            if self.step_id is not None:
                return "{{steps.%s.outputs.parameters.%s}}" % (self.step_id, self.name)
            return "{{outputs.parameters.%s}}" % self.name
        return ""

    def convert_to_argo(self):
        description = None
        if self.type is not None:
            description = jsonpickle.dumps({"type": str(self.type)})

        if self.save_as_artifact:
            if self.value_from_path is not None:
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name, path=self.value_from_path, global_name=self.global_name)
            elif self.value_from_parameter is not None:
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name, _from=str(self.value_from_parameter), global_name=self.global_name)
            elif self.value_from_expression is not None:
                return V1alpha1Artifact(name="dflow_bigpar_" + self.name, from_expression=str(self.value_from_expression), global_name=self.global_name)
            else:
                raise RuntimeError("Not supported.")

        if self.value_from_path is not None:
            return V1alpha1Parameter(name=self.name, value_from=V1alpha1ValueFrom(path=self.value_from_path, default=self.default), global_name=self.global_name, description=description)
        elif self.value_from_parameter is not None:
            return V1alpha1Parameter(name=self.name, value_from=V1alpha1ValueFrom(parameter=str(self.value_from_parameter), default=self.default), global_name=self.global_name, description=description)
        elif self.value_from_expression is not None:
            return V1alpha1Parameter(name=self.name, value_from=V1alpha1ValueFrom(expression=str(self.value_from_expression), default=self.default), global_name=self.global_name, description=description)
        else:
            raise RuntimeError("Output parameter %s is not specified" % self)

class OutputArtifact(ArgoVar):
    """
    Output artifact for OP template

    Args:
        path: path of the output artifact in the container
        _from: the artifact is from another artifact
        name: name of the output artifact
        step_id:
        type: artifact type
        save: place to store the output artifact instead of default storage, can be a list
        archive: compress format of the artifact, None for no compression
        global_name: global name of the artifact within the workflow
        from_expression: the artifact is from an expression
    """
    def __init__(self, path=None, _from=None, name=None, step_id=None, type=None, save=None, archive="tar", global_name=None,
            from_expression=None):
        self.path = path
        self._from = _from
        self.name = name
        self.step_id = step_id
        self.type = type
        if save is None:
            save = []
        elif not isinstance(save, list):
            save = [save]
        self.save = save
        self.archive = archive
        self._sub_path = None
        self.global_name = global_name
        self.from_expression = from_expression
        self.redirect = None

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
                if self.step_id is not None:
                    return "steps['%s'].outputs.artifacts['%s']" % (self.step_id, self.name)
                return "outputs.artifacts['%s']" % self.name
            return ""
        return super().__getattr__(key)

    def __repr__(self):
        if self.redirect is not None:
            return str(self.redirect)
        if self.global_name is not None:
            return "{{workflow.outputs.artifacts.%s}}" % (self.global_name)
        elif self.name is not None:
            if self.step_id is not None:
                return "{{steps.%s.outputs.artifacts.%s}}" % (self.step_id, self.name)
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
                s3 = deepcopy(save)
                if s3._sub_path is not None:
                    if s3.key[-1] != "/": s3.key += "/"
                    s3.key += s3._sub_path

        if self.path is not None:
            return V1alpha1Artifact(name=self.name, path=self.path, archive=archive, s3=s3, global_name=self.global_name)
        elif self._from is not None:
            return V1alpha1Artifact(name=self.name, _from=str(self._from), archive=archive, s3=s3, global_name=self.global_name)
        elif self.from_expression is not None:
            return V1alpha1Artifact(name=self.name, from_expression=str(self.from_expression), archive=archive, s3=s3, global_name=self.global_name)
        else:
            raise RuntimeError("Output artifact %s is not specified" % self)

class PVC:
    def __init__(self, name, subpath, size="1Gi", storage_class=None, access_modes=None):
        self.name = name
        self.subpath = subpath
        self.size = size
        self.storage_class = storage_class
        if access_modes is None:
            access_modes = ["ReadWriteOnce"]
        self.access_modes = access_modes

