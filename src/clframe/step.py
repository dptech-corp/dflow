from copy import deepcopy
from argo.workflows.client import (
    V1alpha1WorkflowStep,
    V1alpha1Arguments,
    V1alpha1Parameter,
    V1alpha1Artifact
)
from .io import (
    InputArtifact,
    InputParameter,
    OutputArtifact,
    OutputParameter
)

class Step:
    def __init__(self, name, template, parameters=None, artifacts=None, when=None):
        self.name = name
        self.id = "steps.%s" % self.name
        self.template = template
        self.inputs = deepcopy(self.template.inputs)
        self.outputs = deepcopy(self.template.outputs)
        self.inputs.set_step_id(self.id)
        self.outputs.set_step_id(self.id)

        if parameters is not None:
            self.set_parameters(parameters)

        if artifacts is not None:
            self.set_artifacts(artifacts)

        self.when = when

    def __repr__(self):
        return self.id
    
    def set_parameters(self, parameters):
        for k, v in parameters.items():
            if isinstance(v, InputParameter) or isinstance(v, OutputParameter):
                self.inputs.parameters[k].value = "{{" + str(v) + "}}"
            else:
                self.inputs.parameters[k].value = v
    
    def set_artifacts(self, artifacts):
        for k, v in artifacts.items():
            if isinstance(v, InputArtifact) or isinstance(v, OutputArtifact):
                self.inputs.artifacts[k].source = "{{" + str(v) + "}}"
            else:
                self.inputs.artifacts[k].source = v

    def convert_to_argo(self):
        return V1alpha1WorkflowStep(
            name=self.name, template=self.template.name, arguments=V1alpha1Arguments(
                parameters=[V1alpha1Parameter(
                    name=k, value=v.value) for k, v in self.inputs.parameters.items()],
                artifacts=[V1alpha1Artifact(
                    name=k, _from=v.source) for k, v in self.inputs.artifacts.items()]
            ), when=self.when
        )
