from argo.workflows.client import (
    V1Container,
    V1alpha1Template,
    V1alpha1ScriptTemplate,
    V1alpha1Parameter,
    V1alpha1Inputs,
    V1alpha1Outputs,
    V1alpha1ValueFrom,
    V1alpha1Artifact
)
from .io import Inputs, Outputs

class OPTemplate:
    def __init__(self, name, inputs=None, outputs=None):
        self.name = name
        if inputs is not None:
            self.inputs = inputs
        else:
            self.inputs = Inputs()
        if outputs is not None:
            self.outputs = outputs
        else:
            self.outputs = Outputs()

class ContainerOPTemplate(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, args=None):
        super().__init__(name=name, inputs=inputs, outputs=outputs)
        self.image = image
        self.command = command
        self.args = args

    def convert_to_argo(self):
        return V1alpha1Template(name=self.name,
            inputs=V1alpha1Inputs(parameters=[
                V1alpha1Parameter(name=k, value=v.value) for k, v in self.inputs.parameters.items()],
                artifacts=[V1alpha1Artifact(name=k, path=v.path, _from=v.source) for k, v in self.inputs.artifacts.items()]),
            outputs=V1alpha1Outputs(parameters=[
                V1alpha1Parameter(name=k, value_from=
                V1alpha1ValueFrom(path=v.value_from_path)) for k, v in self.outputs.parameters.items()],
                artifacts=[V1alpha1Artifact(name=k, path=v.path) for k, v in self.outputs.artifacts.items()]),
            container=V1Container(image=self.image, command=self.command, args=self.args))

class ScriptOPTemplate(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None):
        super().__init__(name=name, inputs=inputs, outputs=outputs)
        self.image = image
        self.command = command
        self.script = script
    
    def convert_to_argo(self):
        return V1alpha1Template(name=self.name,
            inputs=V1alpha1Inputs(parameters=[
                V1alpha1Parameter(name=k, value=v.value) for k, v in self.inputs.parameters.items()],
                artifacts=[V1alpha1Artifact(name=k, path=v.path, _from=v.source) for k, v in self.inputs.artifacts.items()]),
            outputs=V1alpha1Outputs(parameters=[
                V1alpha1Parameter(name=k, value_from=
                V1alpha1ValueFrom(path=v.value_from_path)) for k, v in self.outputs.parameters.items()],
                artifacts=[V1alpha1Artifact(name=k, path=v.path) for k, v in self.outputs.artifacts.items()]),
            script=V1alpha1ScriptTemplate(image=self.image, command=self.command, source=self.script))
