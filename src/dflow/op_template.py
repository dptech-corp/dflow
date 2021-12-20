from argo.workflows.client import (
    V1alpha1Template,
    V1alpha1ScriptTemplate,
    V1VolumeMount,
    V1alpha1Metadata
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

class ShellOPTemplate(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None, mounts=None, init_progress="0/1"):
        super().__init__(name=name, inputs=inputs, outputs=outputs)
        self.image = image
        if command is None:
            command = ["sh"]
        self.command = command
        self.script = script
        if mounts is None:
            mounts = []
        self.mounts = mounts
        self.init_progress = init_progress

    def convert_to_argo(self):
        return V1alpha1Template(name=self.name,
            metadata=V1alpha1Metadata(annotations={"workflows.argoproj.io/progress": self.init_progress}),
            inputs=self.inputs.convert_to_argo(),
            outputs=self.outputs.convert_to_argo(),
            script=V1alpha1ScriptTemplate(image=self.image, command=self.command, source=self.script, volume_mounts=self.mounts))

class PythonScriptOPTemplate(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None, mounts=None, init_progress="0/1"):
        super().__init__(name=name, inputs=inputs, outputs=outputs)
        self.image = image
        if command is None:
            command = ["python"]
        self.command = command
        self.script = script
        if mounts is None:
            mounts = []
        self.mounts = mounts
        self.init_progress = init_progress

    def convert_to_argo(self):
        return V1alpha1Template(name=self.name,
            metadata=V1alpha1Metadata(annotations={"workflows.argoproj.io/progress": self.init_progress}),
            inputs=self.inputs.convert_to_argo(),
            outputs=self.outputs.convert_to_argo(),
            script=V1alpha1ScriptTemplate(image=self.image, command=self.command, source=self.script, volume_mounts=self.mounts))
