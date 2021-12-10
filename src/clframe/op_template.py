from argo.workflows.client import (
    V1alpha1Template,
    V1alpha1ScriptTemplate
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
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None):
        super().__init__(name=name, inputs=inputs, outputs=outputs)
        self.image = image
        if command is None:
            command = ["sh"]
        self.command = command
        self.script = script
    
    def convert_to_argo(self):
        return V1alpha1Template(name=self.name,
            inputs=self.inputs.convert_to_argo(),
            outputs=self.outputs.convert_to_argo(),
            script=V1alpha1ScriptTemplate(image=self.image, command=self.command, source=self.script))

class PythonScriptOPTemplate(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None):
        super().__init__(name=name, inputs=inputs, outputs=outputs)
        self.image = image
        if command is None:
            command = ["python"]
        self.command = command
        self.script = script

    def convert_to_argo(self):
        return V1alpha1Template(name=self.name,
            inputs=self.inputs.convert_to_argo(),
            outputs=self.outputs.convert_to_argo(),
            script=V1alpha1ScriptTemplate(image=self.image, command=self.command, source=self.script))
