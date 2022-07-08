from typing import List

from .io import OutputArtifact, OutputParameter
from .op_template import OPTemplate
from .step import Step

try:
    from argo.workflows.client import (V1alpha1Arguments, V1alpha1ContinueOn,
                                       V1alpha1DAGTask)
except Exception:
    pass


class Task(Step):
    """
    Task

    Args:
        name: the name of the task
        template: OP template the task uses
        parameters: input parameters passed to the task as arguments
        artifacts: input artifacts passed to the task as arguments
        when: conditional task if the condition is satisfied
        with_param: generate parallel tasks with respect to a list as
            a parameter
        continue_on_failed: continue if the task fails
        continue_on_num_success: continue if the success number of the
            generated parallel tasks greater than certain number
        continue_on_success_ratio: continue if the success ratio of the
            generated parallel tasks greater than certain number
        with_sequence: generate parallel tasks with respect to a sequence
        key: the key of the task
        executor: define the executor to execute the script
        use_resource: use k8s resource
        util_image: image for utility step
        util_command: command for utility step
        dependencies: extra dependencies of the task
    """

    def __init__(
            self,
            name: str,
            template: OPTemplate,
            dependencies: List["Task"] = None,
            **kwargs,
    ) -> None:
        # work around circular import problem
        self.is_task = True
        if dependencies is None:
            dependencies = []
        self.dependencies = dependencies
        super().__init__(name=name, template=template, **kwargs)
        if self.prepare_step is not None:
            self.dependencies.append(self.prepare_step)
        if self.check_step is not None:
            self.check_step.dependencies.append(self)

    def set_parameters(self, parameters):
        super().set_parameters(parameters)
        for v in parameters.values():
            if isinstance(v, (OutputParameter, OutputArtifact)) and v.step \
                    not in self.dependencies:
                self.dependencies.append(v.step)

    def set_artifacts(self, artifacts):
        super().set_artifacts(artifacts)
        for v in artifacts.values():
            if isinstance(v, OutputArtifact) and v.step not in \
                    self.dependencies:
                self.dependencies.append(v.step)

    def convert_to_argo(self, context=None):
        self.prepare_argo_arguments(context)
        return V1alpha1DAGTask(
            name=self.name, template=self.template.name,
            arguments=V1alpha1Arguments(
                parameters=self.argo_parameters,
                artifacts=self.argo_artifacts
            ), when=self.when, with_param=self.with_param,
            with_sequence=self.with_sequence,
            continue_on=V1alpha1ContinueOn(failed=self.continue_on_failed),
            dependencies=[task.name for task in self.dependencies],
        )
