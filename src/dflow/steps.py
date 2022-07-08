from typing import Dict, List, Union

from .io import Inputs, Outputs
from .op_template import OPTemplate
from .step import Step

try:
    from argo.workflows.client import V1alpha1Metadata, V1alpha1Template
except Exception:
    pass


class Steps(OPTemplate):
    """
    Steps

    Args:
        name: the name of the steps
        inputs: inputs in the template
        outputs: outputs in the template
        steps: a sequential list of steps
        memoize_key: memoized key of the steps
        annotations: annotations for the OP template
        """

    def __init__(
            self,
            name: str = None,
            inputs: Inputs = None,
            outputs: Outputs = None,
            steps: List[Union[Step, List[Step]]] = None,
            memoize_key: str = None,
            annotations: Dict[str, str] = None,
    ) -> None:
        super().__init__(name=name, inputs=inputs, outputs=outputs,
                         memoize_key=memoize_key, annotations=annotations)
        if steps is not None:
            self.steps = steps
        else:
            self.steps = []

    def __iter__(self):
        return iter(self.steps)

    def add(
            self,
            step: Union[Step, List[Step]],
    ) -> None:
        """
        Add a step or a list of parallel steps to the steps

        Args:
            step: a step or a list of parallel steps to be added to the
                entrypoint of the workflow
        """
        self.steps.append(step)

    def convert_to_argo(self, memoize_prefix=None,
                        memoize_configmap="dflow-config", context=None):
        argo_steps = []
        templates = []
        assert len(self.steps) > 0, "Steps %s is empty" % self.name
        for step in self.steps:
            # each step of steps should be a list of parallel steps, if not,
            # create a sigleton
            if not isinstance(step, list):
                step = [step]
            argo_prepare_steps = []
            argo_parallel_steps = []
            argo_check_steps = []
            for ps in step:
                assert isinstance(ps, Step)
                if ps.prepare_step is not None:
                    argo_prepare_steps.append(
                        ps.prepare_step.convert_to_argo(context))
                    templates.append(ps.prepare_step.template)
                argo_parallel_steps.append(ps.convert_to_argo(context))
                # template may change after conversion
                templates.append(ps.template)
                if ps.check_step is not None:
                    argo_check_steps.append(
                        ps.check_step.convert_to_argo(context))
                    templates.append(ps.check_step.template)
            if argo_prepare_steps:
                argo_steps.append(argo_prepare_steps)
            argo_steps.append(argo_parallel_steps)
            if argo_check_steps:
                argo_steps.append(argo_check_steps)

        self.handle_key(memoize_prefix, memoize_configmap)
        argo_template = \
            V1alpha1Template(name=self.name,
                             metadata=V1alpha1Metadata(
                                 annotations=self.annotations),
                             steps=argo_steps,
                             inputs=self.inputs.convert_to_argo(),
                             outputs=self.outputs.convert_to_argo(),
                             memoize=self.memoize)
        return argo_template, templates
