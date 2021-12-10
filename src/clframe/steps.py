from argo.workflows.client import V1alpha1Template
from .op_template import OPTemplate

class Steps(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, steps=None):
        super().__init__(name=name, inputs=inputs, outputs=outputs)
        if steps is not None:
            self.steps = steps
        else:
            self.steps = []

    def __iter__(self):
        return iter(self.steps)

    def add(self, step):
        self.steps.append(step)

    def convert_to_argo(self):
        argo_steps = []
        templates = []
        for step in self.steps:
            # each step of steps should be a list of parallel steps, if not, create a sigleton
            if not isinstance(step, list):
                step = [step]
            argo_parallel_steps = []
            for ps in step:
                argo_parallel_steps.append(ps.convert_to_argo())
                templates.append(ps.template) # template may change after conversion
            argo_steps.append(argo_parallel_steps)

        argo_template = V1alpha1Template(name=self.name,
                steps=argo_steps,
                inputs=self.inputs.convert_to_argo(),
                outputs=self.outputs.convert_to_argo())
        return argo_template, templates
