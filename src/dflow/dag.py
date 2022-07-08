from typing import Dict, List, Union

from .io import Inputs, Outputs
from .op_template import OPTemplate
from .task import Task

try:
    from argo.workflows.client import (V1alpha1DAGTemplate, V1alpha1Metadata,
                                       V1alpha1Template)
except Exception:
    pass


class DAG(OPTemplate):
    """
    DAG

    Args:
        name: the name of the dag
        inputs: inputs in the template
        outputs: outputs in the template
        tasks: a list of tasks
        memoize_key: memoized key of the dag
        annotations: annotations for the OP template
        """

    def __init__(
            self,
            name: str = None,
            inputs: Inputs = None,
            outputs: Outputs = None,
            tasks: List[Task] = None,
            memoize_key: str = None,
            annotations: Dict[str, str] = None,
    ) -> None:
        super().__init__(name=name, inputs=inputs, outputs=outputs,
                         memoize_key=memoize_key, annotations=annotations)
        if tasks is not None:
            self.tasks = tasks
        else:
            self.tasks = []

    def __iter__(self):
        return iter(self.tasks)

    def add(
            self,
            task: Union[Task, List[Task]],
    ) -> None:
        """
        Add a task or a list of tasks to the dag

        Args:
            task: a task or a list of tasks to be added to the dag
        """
        if not isinstance(task, list):
            assert isinstance(task, Task)
            self.tasks.append(task)
        else:
            for t in task:
                assert isinstance(t, Task)
                self.tasks.append(t)

    def convert_to_argo(self, memoize_prefix=None,
                        memoize_configmap="dflow-config", context=None):
        argo_tasks = []
        templates = []
        assert len(self.tasks) > 0, "Dag %s is empty" % self.name
        for task in self.tasks:
            if task.prepare_step is not None:
                argo_tasks.append(task.prepare_step.convert_to_argo(context))
                templates.append(task.prepare_step.template)
            argo_tasks.append(task.convert_to_argo(context))
            templates.append(task.template)
            if task.check_step is not None:
                argo_tasks.append(task.check_step.convert_to_argo(context))
                templates.append(task.check_step.template)

        self.handle_key(memoize_prefix, memoize_configmap)
        argo_template = \
            V1alpha1Template(name=self.name,
                             metadata=V1alpha1Metadata(
                                 annotations=self.annotations),
                             dag=V1alpha1DAGTemplate(
                                 tasks=argo_tasks,
                             ),
                             inputs=self.inputs.convert_to_argo(),
                             outputs=self.outputs.convert_to_argo(),
                             memoize=self.memoize)
        return argo_template, templates
