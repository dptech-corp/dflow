from typing import Optional, Dict, List, Union

from .config import config, s3_config
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
        parallelism: maximum number of running pods for the OP template
        """

    def __init__(
            self,
            name: Optional[str] = None,
            inputs: Optional[Inputs] = None,
            outputs: Optional[Outputs] = None,
            tasks: Optional[List[Task]] = None,
            memoize_key: Optional[str] = None,
            annotations: Dict[str, str] = None,
            parallelism: Optional[int] = None,
    ) -> None:
        super().__init__(name=name, inputs=inputs, outputs=outputs,
                         memoize_key=memoize_key, annotations=annotations)
        self.parallelism = parallelism
        self.tasks = []
        if tasks is not None:
            for task in tasks:
                self.add(task)

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
            task = [task]

        for t in task:
            assert isinstance(t, Task)
            if t.prepare_step is not None:
                self.tasks.append(t.prepare_step)
            self.tasks.append(t)
            if t.check_step is not None:
                self.tasks.append(t.check_step)

    def convert_to_argo(self, memoize_prefix=None,
                        memoize_configmap="dflow", context=None):
        argo_tasks = []
        templates = []
        assert len(self.tasks) > 0, "Dag %s is empty" % self.name
        for task in self.tasks:
            argo_tasks.append(task.convert_to_argo(context))
            templates.append(task.template)

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
                             memoize=self.memoize,
                             parallelism=self.parallelism)
        return argo_template, templates

    def resolve(self):
        from multiprocessing import Process
        for task in self.waiting:
            ready = True
            for dep in task.dependencies:
                if dep not in self.finished:
                    ready = False
                    break
            if ready:
                task.phase = "Pending"
                i = self.tasks.index(task)
                proc = Process(target=task.run_with_queue,
                               args=(self, i, self.queue, config, s3_config))
                proc.start()
                self.waiting.remove(task)
                self.running.append(task)

    def run(self, workflow_id=None):
        self.workflow_id = workflow_id
        from copy import deepcopy
        from multiprocessing import Queue
        self.queue = Queue()
        self.waiting = [task for task in self]
        self.running = []
        self.finished = []
        self.resolve()

        while len(self.running) > 0:
            # TODO: if the process is killed, this will be blocked forever
            j, t = self.queue.get()
            if t is None:
                self.tasks[j].phase = "Failed"
                if not self.tasks[j].continue_on_failed:
                    raise RuntimeError("Task %s failed" % self.tasks[j])
            else:
                self.tasks[j].outputs = deepcopy(t.outputs)
            self.running.remove(self.tasks[j])
            self.finished.append(self.tasks[j])
            self.resolve()

        assert len(self.finished) == len(self.tasks), "cyclic graph"
