from typing import List, Optional, Union

from .io import OutputArtifact, OutputParameter
from .op_template import OPTemplate
from .step import Step

try:
    from argo.workflows.client import V1alpha1Arguments, V1alpha1ContinueOn
    from .client import V1alpha1DAGTask
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
        util_image_pull_policy: image pull policy for utility step
        util_command: command for utility step
        dependencies: extra dependencies of the task
    """

    def __init__(
            self,
            name: str,
            template: OPTemplate,
            dependencies: Optional[List[Union["Task", str]]] = None,
            **kwargs,
    ) -> None:
        self.dependencies = []
        super().__init__(name=name, template=template, **kwargs)
        # override inferred dependencies if specified explicitly
        if dependencies is not None:
            self.dependencies = dependencies
        if self.prepare_step is not None:
            self.dependencies.append(self.prepare_step)
        if self.check_step is not None:
            self.check_step.dependencies = [
                "%s.Succeeded || %s.Failed || %s.Errored" % (self, self, self)]

    @property
    def expr(self):
        return "tasks['%s']" % self.id

    @classmethod
    def from_dict(cls, d, templates):
        task = super().from_dict(d, templates)
        task.dependencies = d.get("dependencies", [])
        if d.get("depends"):
            for dep in d["depends"].split("&&"):
                dep = dep.strip()
                # removeprefix and removesuffix only supported for python>=3.9
                dep = dep[1:] if dep.startswith("(") else dep
                dep = dep[:-1] if dep.endswith(")") else dep
                dep = dep[:-10] if dep.endswith(".Succeeded") else dep
                task.dependencies.append(dep)
        return task

    def set_parameters(self, parameters):
        super().set_parameters(parameters)
        for v in self.inputs.parameters.values():
            if hasattr(v, "value"):
                def handle(obj):
                    # TODO: Only support output parameter, dict and list
                    if isinstance(obj, OutputParameter):
                        if obj.step not in self.dependencies:
                            self.dependencies.append(obj.step)
                    elif isinstance(obj, dict):
                        for v in obj.values():
                            handle(v)
                    elif isinstance(obj, list):
                        for v in obj:
                            handle(v)
                handle(v.value)

    def set_artifacts(self, artifacts):
        super().set_artifacts(artifacts)
        for v in self.inputs.artifacts.values():
            if isinstance(v.source, OutputArtifact) and v.source.step not in \
                    self.dependencies:
                self.dependencies.append(v.source.step)

    def convert_to_argo(self, context=None):
        self.prepare_argo_arguments(context)
        depends = []
        for task in self.dependencies:
            if isinstance(task, Task):
                if task.check_step is not None:
                    depends.append("(%s.Succeeded)" % task.check_step)
                else:
                    depends.append("(%s.Succeeded)" % task)
            else:
                depends.append("(%s)" % task)
        if self.continue_on_failed or self.continue_on_error:
            continue_on = V1alpha1ContinueOn(
                failed=self.continue_on_failed, error=self.continue_on_error)
        else:
            continue_on = None
        return V1alpha1DAGTask(
            name=self.name, template=self.template.name,
            arguments=V1alpha1Arguments(
                parameters=self.argo_parameters,
                artifacts=self.argo_artifacts
            ), when=self.when, with_param=self.with_param,
            continue_on=continue_on,
            with_sequence=self.with_sequence,
            depends=" && ".join(depends),
            hooks={
                name: hook.convert_to_argo(context)
                for name, hook in self.hooks.items()
            },
        )

    def convert_to_graph(self):
        g = super().convert_to_graph()
        g["dependencies"] = [str(d) for d in self.dependencies]
        return g
