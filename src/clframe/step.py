from copy import deepcopy
from argo.workflows.client import (
    V1alpha1WorkflowStep,
    V1alpha1Arguments
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
            self.inputs.parameters[k].value = v
    
    def set_artifacts(self, artifacts):
        for k, v in artifacts.items():
            self.inputs.artifacts[k].source = v

    def convert_to_argo(self):
        argo_parameters = []
        for par in self.inputs.parameters.values():
            argo_parameters.append(par.convert_to_argo())

        argo_artifacts = []
        for art in self.inputs.artifacts.values():
            argo_artifacts.append(art.convert_to_argo())

        return V1alpha1WorkflowStep(
            name=self.name, template=self.template.name, arguments=V1alpha1Arguments(
                parameters=argo_parameters,
                artifacts=argo_artifacts
            ), when=self.when
        )
