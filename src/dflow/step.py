import logging
import re
from copy import deepcopy
from typing import Any, Dict, List, Union

import jsonpickle

from .common import S3Artifact
from .config import config
from .executor import Executor
from .io import (PVC, ArgoVar, InputArtifact, InputParameter, OutputArtifact,
                 OutputParameter)
from .op_template import OPTemplate, PythonScriptOPTemplate, ShellOPTemplate
from .resource import Resource
from .util_ops import CheckNumSuccess, CheckSuccessRatio
from .utils import catalog_of_artifact, upload_artifact

try:
    from argo.workflows.client import (V1alpha1Arguments, V1alpha1ContinueOn,
                                       V1alpha1ResourceTemplate,
                                       V1alpha1WorkflowStep, V1VolumeMount)

    from .client import V1alpha1Sequence
except Exception:
    V1alpha1Sequence = object


uploaded_python_packages = []


def argo_range(
        *args,
) -> ArgoVar:
    """
    Return a str representing a range of integer in Argo
    It receives 1-3 arguments, which is similar to the function `range` in
        Python
    Each argument can be Argo parameter
    """
    start = 0
    step = 1
    if len(args) == 1:
        end = args[0]
    elif len(args) == 2:
        start = args[0]
        end = args[1]
    elif len(args) == 3:
        start = args[0]
        end = args[1]
        step = args[2]
    else:
        raise TypeError("Expected 1-3 arguments, got %s" % len(args))
    if isinstance(start, (InputParameter, OutputParameter)):
        start = "sprig.atoi(%s)" % start.expr
    if isinstance(step, (InputParameter, OutputParameter)):
        step = "sprig.atoi(%s)" % step.expr
    if isinstance(end, (InputParameter, OutputParameter)):
        end = "sprig.atoi(%s)" % end.expr
    return ArgoVar("toJson(sprig.untilStep(%s, %s, %s))" % (start, end, step))


def argo_sequence(
        count: Union[int, ArgoVar] = None,
        start: Union[int, ArgoVar] = None,
        end: Union[int, ArgoVar] = None,
        format: str = None,
) -> V1alpha1Sequence:
    """
    Return a numeric sequence in Argo

    Args:
        count: number of elements in the sequence (default: 0), not to be used
            with end, can be an Argo parameter
        start: number at which to start the sequence (default: 0), can be an
            Argo parameter
        end: number at which to end the sequence (default: 0), not to be used
            with count, can be an Argo parameter
        format: a printf format string to format the value in the sequence
    """
    if isinstance(count, ArgoVar):
        count = "{{=%s}}" % count.expr
    if isinstance(start, ArgoVar):
        start = "{{=%s}}" % start.expr
    if isinstance(end, ArgoVar):
        end = "{{=%s}}" % end.expr
    return V1alpha1Sequence(count=count, start=start, end=end, format=format)


def argo_len(
        param: Union[ArgoVar, S3Artifact],
) -> ArgoVar:
    """
    Return the length of a list which is an Argo parameter

    Args:
        param: the Argo parameter which is a list
    """
    if isinstance(param, S3Artifact):
        try:
            path_list = catalog_of_artifact(param)
            if path_list:
                param.path_list = path_list
        except Exception:
            pass
        return ArgoVar(str(len(param.path_list)))
    if isinstance(param, InputArtifact):
        assert config["save_path_as_parameter"]
        return ArgoVar("len(sprig.fromJson(%s))" %
                       param.get_path_list_parameter())
    elif isinstance(param, OutputArtifact):
        assert config["save_path_as_parameter"]
        return ArgoVar("len(sprig.fromJson(%s))" %
                       param.get_path_list_parameter())
    else:
        return ArgoVar("len(sprig.fromJson(%s))" % param.expr)


class Step:
    """
    Step

    Args:
        name: the name of the step
        template: OP template the step uses
        parameters: input parameters passed to the step as arguments
        artifacts: input artifacts passed to the step as arguments
        when: conditional step if the condition is satisfied
        with_param: generate parallel steps with respect to a list as a
            parameter
        continue_on_failed: continue if the step fails
        continue_on_num_success: continue if the success number of the
            generated parallel steps greater than certain number
        continue_on_success_ratio: continue if the success ratio of the
            generated parallel steps greater than certain number
        with_sequence: generate parallel steps with respect to a sequence
        key: the key of the step
        executor: define the executor to execute the script
        use_resource: use k8s resource
        util_image: image for utility step
        util_command: command for utility step
    """

    def __init__(
            self,
            name: str,
            template: OPTemplate,
            parameters: Dict[str, Any] = None,
            artifacts: Dict[str, Union[S3Artifact,
                                       InputArtifact, OutputArtifact]] = None,
            when: str = None,
            with_param: Union[str, list,
                              InputParameter, OutputParameter] = None,
            continue_on_failed: bool = False,
            continue_on_num_success: int = None,
            continue_on_success_ratio: float = None,
            with_sequence: V1alpha1Sequence = None,
            key: str = None,
            executor: Executor = None,
            use_resource: Resource = None,
            util_image: str = None,
            util_command: Union[str, List[str]] = None,
            **kwargs,
    ) -> None:
        self.name = name
        self.id = self.name
        self.template = template
        self.inputs = deepcopy(self.template.inputs)
        self.outputs = deepcopy(self.template.outputs)
        self.inputs.set_step(self)
        self.outputs.set_step(self)
        self.continue_on_failed = continue_on_failed
        self.continue_on_num_success = continue_on_num_success
        self.continue_on_success_ratio = continue_on_success_ratio
        self.check_step = None
        self.prepare_step = None

        if parameters is not None:
            self.set_parameters(parameters)

        if artifacts is not None:
            self.set_artifacts(artifacts)

        self.when = when
        self.with_param = with_param
        self.with_sequence = with_sequence
        self.key = key
        self.executor = executor
        self.use_resource = use_resource
        if util_image is None:
            util_image = config["util_image"]
        self.util_image = util_image
        if isinstance(util_command, str):
            util_command = [util_command]
        self.util_command = util_command

        if hasattr(self.template, "python_packages"):
            hit = list(filter(lambda x: x[0] == self.template.python_packages,
                              uploaded_python_packages))
            if len(hit) > 0:
                self.set_artifacts({"dflow_python_packages": hit[0][1]})
            else:
                artifact = upload_artifact(self.template.python_packages)
                self.set_artifacts({"dflow_python_packages": artifact})
                uploaded_python_packages.append(
                    (self.template.python_packages, artifact))

        if self.key is not None:
            self.template.inputs.parameters["dflow_key"] = InputParameter(
                value="")
            self.inputs.parameters["dflow_key"] = InputParameter(
                value=str(self.key))

        if hasattr(self.template, "slices") and self.template.slices is not \
                None and (self.template.slices.output_artifact or (
                    self.template.slices.sub_path and
                    self.template.slices.input_artifact)):
            new_template = deepcopy(self.template)
            new_template.name = self.template.name + "-" + self.name
            script = "import os, json\n"
            for name in new_template.slices.output_artifact:
                script += "os.makedirs('/tmp/outputs/artifacts/%s/%s', "\
                    "exist_ok=True)\n" % (name, config["catalog_dir_name"])
                script += "with open('/tmp/outputs/artifacts/%s/%s/init',"\
                    " 'w') as f:\n" % (name, config["catalog_dir_name"])
                script += "    json.dump({'path_list': []}, f)\n"
            init_template = PythonScriptOPTemplate(
                name="%s-init-artifact" % new_template.name,
                image=self.util_image, command=self.util_command,
                image_pull_policy=new_template.image_pull_policy,
                script=script)
            if self.key is not None:
                new_template.inputs.parameters["dflow_group_key"] = \
                    InputParameter(value="")
                self.inputs.parameters["dflow_group_key"] = InputParameter(
                    value=re.sub("{{item.*}}", "group", str(self.key)))
                init_template.inputs.parameters["dflow_group_key"] = \
                    InputParameter()
                # For the case of reusing sliced steps, ensure that the output
                # artifacts are reused
                for name in new_template.slices.output_artifact:
                    init_template.outputs.artifacts[name] = OutputArtifact(
                        path="/tmp/outputs/artifacts/%s" % name,
                        save=S3Artifact(key="{{workflow.name}}/{{inputs."
                                        "parameters.dflow_group_key}}/%s"
                                        % name), archive=None)
                    new_template.outputs.artifacts[name].save.append(
                        S3Artifact(key="{{workflow.name}}/{{inputs."
                                   "parameters.dflow_group_key}}/%s" % name))
            else:
                init_template.outputs.parameters["dflow_artifact_key"] = \
                    OutputParameter(value="{{workflow.name}}/{{pod.name}}")
                new_template.inputs.parameters["dflow_artifact_key"] = \
                    InputParameter(value="")
                for name in new_template.slices.output_artifact:
                    init_template.outputs.artifacts[name] = OutputArtifact(
                        path="/tmp/outputs/artifacts/%s" % name,
                        save=S3Artifact(key="{{workflow.name}}/{{pod.name}}/%s"
                                        % name), archive=None)
                    new_template.outputs.artifacts[name].save.append(
                        S3Artifact(key="{{inputs.parameters."
                                   "dflow_artifact_key}}/%s" % name))

            if new_template.slices.sub_path and \
                    new_template.slices.input_artifact:
                for i, name in enumerate(new_template.slices.input_artifact):
                    init_template.inputs.artifacts[name] = InputArtifact(
                        path="/tmp/inputs/artifacts/%s" % name,
                        optional=True, sub_path=config["catalog_dir_name"])
                    init_template.outputs.parameters["dflow_slices_path"] = \
                        OutputParameter(value_from_path="/tmp/outputs/"
                                        "parameters/dflow_slices_path")
                    init_template.script += "path_list_%s = []\n" % i
                    init_template.script += \
                        "path = '/tmp/inputs/artifacts/%s'\n" % name
                    init_template.script += \
                        "if os.path.exists(path):\n"
                    init_template.script += \
                        "    for f in os.listdir(path):\n"
                    init_template.script += \
                        "        with open(os.path.join(path, f), 'r') as fd:"\
                        "\n"
                    init_template.script += \
                        "            path_list_%s += json.load(fd)['path_list"\
                        "']\n" % i
                    init_template.script += "path_list_%s.sort(key=lambda x: "\
                        "x['order'])\n" % i
                n_arts = len(new_template.slices.input_artifact)
                if n_arts > 1:
                    init_template.script += "assert " + \
                        " == ".join(["len(path_list_%s)" %
                                    i for i in range(n_arts)]) + "\n"
                init_template.script += "slices_path = []\n"
                init_template.script += "for i in range(len(path_list_0)):\n"
                init_template.script += "    item = {'order': i}\n"
                for i, name in enumerate(new_template.slices.input_artifact):
                    init_template.script += "    item['%s'] = path_list_%s[i]"\
                        "['dflow_list_item']\n" % (name, i)
                init_template.script += "    slices_path.append(item)\n"
                init_template.script += "os.makedirs('/tmp/outputs/"\
                    "parameters', exist_ok=True)\n"
                init_template.script += "with open('/tmp/outputs/parameters/"\
                    "dflow_slices_path', 'w') as f:\n"
                init_template.script += "    json.dump(slices_path, f)\n"

            if self.key is not None:
                self.prepare_step = self.__class__(
                    name="%s-init-artifact" % self.name,
                    template=init_template,
                    parameters={"dflow_group_key": re.sub("{{item.*}}",
                                                          "group",
                                                          str(self.key))})
            else:
                self.prepare_step = self.__class__(
                    name="%s-init-artifact" % self.name,
                    template=init_template)

            if key is None:
                self.inputs.parameters["dflow_artifact_key"] = InputParameter(
                    value=self.prepare_step.outputs.parameters[
                        "dflow_artifact_key"])

            if new_template.slices.sub_path and \
                    new_template.slices.input_artifact:
                for name in new_template.slices.input_artifact:
                    self.inputs.parameters["dflow_%s_sub_path" %
                                           name].value = "{{item.%s}}" % name
                    # step cannot resolve
                    # {{inputs.parameters.dflow_%s_sub_path}}
                    self.inputs.artifacts[name].path = None
                    v = self.inputs.artifacts[name].source
                    if isinstance(v, S3Artifact):
                        self.prepare_step.inputs.artifacts[name].source = \
                            v.sub_path(config["catalog_dir_name"])
                        self.inputs.artifacts[name].source = \
                            v.sub_path("{{item.%s}}" % name)
                    elif isinstance(v, (InputArtifact, OutputArtifact)):
                        self.prepare_step.inputs.artifacts[name].source = v
                        self.inputs.artifacts[name].sub_path = \
                            "{{item.%s}}" % name
                self.with_param = self.prepare_step.outputs.parameters[
                    "dflow_slices_path"]

            for name in new_template.slices.output_artifact:
                self.outputs.artifacts[name].redirect = \
                    self.prepare_step.outputs.artifacts[name]
            self.template = new_template

    def __repr__(self):
        return self.id

    def set_parameters(self, parameters):
        for k, v in parameters.items():
            # if a parameter is saved as artifact, the parameters it pass
            # value to or its value comes from must be saved as artifact
            # as well
            if isinstance(v, (InputParameter, OutputParameter)):
                if self.inputs.parameters[k].type is None and v.type is not \
                        None:
                    self.inputs.parameters[k].type = v.type
                if v.type is None and self.inputs.parameters[k].type is not \
                        None:
                    v.type = self.inputs.parameters[k].type

                if self.inputs.parameters[k].save_as_artifact:
                    v.save_as_artifact = True
                if v.save_as_artifact:
                    self.inputs.parameters[k].save_as_artifact = True

            if self.inputs.parameters[k].save_as_artifact and isinstance(v, (
                    InputParameter, OutputParameter, InputArtifact,
                    OutputArtifact)):
                self.inputs.parameters[k].source = v
                return

            if v is None:
                self.inputs.parameters[k].value = "null"
            else:
                self.inputs.parameters[k].value = v

    def set_artifacts(self, artifacts):
        for k, v in artifacts.items():
            if v is None:
                del self.inputs.artifacts[k]
                self.template.inputs.artifacts[k].optional = True
            else:
                self.inputs.artifacts[k].source = v
                if config["save_path_as_parameter"]:
                    if isinstance(v, S3Artifact) and v.path_list is not None:
                        try:
                            path_list = catalog_of_artifact(v)
                            if path_list:
                                v.path_list = path_list
                        except Exception:
                            pass
                        self.inputs.parameters["dflow_%s_path_list" % k] = \
                            InputParameter(value=v.path_list)
                    elif isinstance(v, OutputArtifact) and v.step is not None \
                            and "dflow_%s_path_list" % v.name in \
                                v.step.outputs.parameters:
                        self.inputs.parameters["dflow_%s_path_list" % k] = \
                            InputParameter(
                            value=v.step.outputs.parameters[
                                "dflow_%s_path_list" % v.name])
                    elif isinstance(v, InputArtifact) and v.template is not \
                            None and "dflow_%s_path_list" % v.name in \
                            v.template.inputs.parameters:
                        self.inputs.parameters["dflow_%s_path_list" % k] = \
                            InputParameter(
                            value=v.template.inputs.parameters[
                                "dflow_%s_path_list" % v.name])

    def prepare_argo_arguments(self, context=None):
        self.argo_parameters = []
        self.argo_artifacts = []
        for par in self.inputs.parameters.values():
            if par.save_as_artifact:
                self.argo_artifacts.append(par.convert_to_argo())
            else:
                self.argo_parameters.append(par.convert_to_argo())

        new_template = None

        pvc_arts = []
        for art in self.inputs.artifacts.values():
            if isinstance(art.source, PVC):
                pvc_arts.append((art.source, art))
            elif art.source is None and art.optional:
                pass
            else:
                self.argo_artifacts.append(art.convert_to_argo())

        if len(pvc_arts) > 0:
            if new_template is None:
                new_template = deepcopy(self.template)
                new_template.name = self.template.name + "-" + self.name
            if (isinstance(new_template, ShellOPTemplate)):
                for pvc, art in pvc_arts:
                    del new_template.inputs.artifacts[art.name]
                    new_template.script = "ln -s /tmp/mnt/%s %s\n" % (
                        pvc.subpath, art.path) + new_template.script
                    new_template.mounts.append(V1VolumeMount(
                        name=pvc.name, mount_path="/tmp/mnt"))
                    new_template.pvcs.append(pvc)
            elif (isinstance(new_template, PythonScriptOPTemplate)):
                for pvc, art in pvc_arts:
                    del new_template.inputs.artifacts[art.name]
                    new_template.script = \
                        "os.system('ln -s /tmp/mnt/%s %s')\n" % (
                            pvc.subpath, art.path) + new_template.script
                    new_template.mounts.append(V1VolumeMount(
                        name=pvc.name, mount_path="/tmp/mnt"))
                    new_template.pvcs.append(pvc)
                new_template.script = "import os\n" + new_template.script
            else:
                raise RuntimeError(
                    "Unsupported type of OPTemplate to mount PVC")

        pvc_arts = []
        for art in self.outputs.artifacts.values():
            for save in art.save:
                if isinstance(save, PVC):
                    pvc_arts.append((save, art))

        if len(pvc_arts) > 0:
            if new_template is None:
                new_template = deepcopy(self.template)
                new_template.name = self.template.name + "-" + self.name
            if (isinstance(new_template, ShellOPTemplate)):
                new_template.script += "\n"
                for pvc, art in pvc_arts:
                    new_template.mounts.append(V1VolumeMount(
                        name=pvc.name, mount_path="/tmp/mnt"))
                    new_template.script += "cp -r %s /tmp/mnt/%s\n" % (
                        art.path, pvc.subpath)
                    new_template.pvcs.append(pvc)
            elif (isinstance(new_template, PythonScriptOPTemplate)):
                new_template.script += "\nimport os\n"
                for pvc, art in pvc_arts:
                    new_template.mounts.append(V1VolumeMount(
                        name=pvc.name, mount_path="/tmp/mnt"))
                    new_template.script += \
                        "os.system('cp -r %s /tmp/mnt/%s')\n" % (
                            art.path, pvc.subpath)
                    new_template.pvcs.append(pvc)
            else:
                raise RuntimeError(
                    "Unsupported type of OPTemplate to mount PVC")

        if self.continue_on_num_success or self.continue_on_success_ratio is \
                not None:
            self.continue_on_failed = True
            if new_template is None:
                new_template = deepcopy(self.template)
                new_template.name = self.template.name + "-" + self.name
            if (isinstance(new_template, ShellOPTemplate)):
                new_template.outputs.parameters["dflow_success_tag"] = \
                    OutputParameter(value_from_path="/tmp/success_tag",
                                    default="0")
                new_template.script += "\n"
                new_template.script += "echo 1 > /tmp/success_tag\n"
            elif (isinstance(new_template, PythonScriptOPTemplate)):
                new_template.outputs.parameters["dflow_success_tag"] = \
                    OutputParameter(value_from_path="/tmp/success_tag",
                                    default="0")
                new_template.script += "\n"
                new_template.script += "with open('/tmp/success_tag', 'w')"\
                    " as f:\n    f.write('1')\n"
            else:
                raise RuntimeError(
                    "Unsupported type of OPTemplate for "
                    "continue_on_num_success or continue_on_success_ratio")

        if new_template is not None:
            self.template = new_template
            self.outputs = deepcopy(self.template.outputs)
            self.outputs.set_step(self)

        if self.continue_on_num_success is not None:
            self.check_step = self.__class__(
                name="%s-check-num-success" % self.name,
                template=CheckNumSuccess(image=self.util_image),
                parameters={
                    "success": self.outputs.parameters["dflow_success_tag"],
                    "threshold": self.continue_on_num_success
                }
            )
        elif self.continue_on_success_ratio is not None:
            self.check_step = self.__class__(
                name="%s-check-success-ratio" % self.name,
                template=CheckSuccessRatio(image=self.util_image),
                parameters={
                    "success": self.outputs.parameters["dflow_success_tag"],
                    "threshold": self.continue_on_success_ratio
                }
            )

        if isinstance(self.with_param, ArgoVar):
            self.with_param = "{{=%s}}" % self.with_param.expr
        elif self.with_param is not None and not isinstance(self.with_param,
                                                            str):
            self.with_param = jsonpickle.dumps(list(self.with_param))

        if context is not None:
            self.template = context.render(self.template)

        if self.executor is not None:
            assert isinstance(self.executor, Executor)
            self.template = self.executor.render(self.template)

        if self.use_resource is not None:
            self.template.resource = V1alpha1ResourceTemplate(
                action=self.use_resource.action,
                success_condition=self.use_resource.success_condition,
                failure_condition=self.use_resource.failure_condition,
                manifest=self.use_resource.get_manifest(self.template.command,
                                                        self.template.script))

    def convert_to_argo(self, context=None):
        logging.debug("handle step %s" % self.name)
        self.prepare_argo_arguments(context)
        return V1alpha1WorkflowStep(
            name=self.name, template=self.template.name,
            arguments=V1alpha1Arguments(
                parameters=self.argo_parameters,
                artifacts=self.argo_artifacts
            ), when=self.when, with_param=self.with_param,
            with_sequence=self.with_sequence,
            continue_on=V1alpha1ContinueOn(failed=self.continue_on_failed)
        )
