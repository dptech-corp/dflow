import logging
import os
import re
import sys
from copy import deepcopy
from typing import Any, Dict, List, Optional, Union

import jsonpickle

from .common import LocalArtifact, S3Artifact
from .config import config, s3_config
from .context_syntax import GLOBAL_CONTEXT
from .executor import Executor
from .io import (PVC, ArgoVar, Expression, InputArtifact, InputParameter,
                 OutputArtifact, OutputParameter, if_expression, to_expr)
from .op_template import (OPTemplate, PythonScriptOPTemplate, ScriptOPTemplate,
                          ShellOPTemplate)
from .python import Slices
from .resource import Resource
from .util_ops import CheckNumSuccess, CheckSuccessRatio, InitArtifactForSlices
from .utils import catalog_of_artifact, merge_dir, randstr, upload_artifact

try:
    from argo.workflows.client import (V1alpha1Arguments, V1alpha1ContinueOn,
                                       V1alpha1ResourceTemplate,
                                       V1alpha1WorkflowStep, V1VolumeMount)

    from .client import V1alpha1Sequence
except Exception:
    V1alpha1Sequence = object


uploaded_python_packages = []


def add_prefix_to_slices(prefix, slices):
    return "(lambda x: ['%s.' + str(i) for i in x] "\
        "if isinstance(x, list) else '%s.' + str(x))(%s)" % (
            prefix, prefix, slices)


class ArgoRange(ArgoVar):
    def __init__(self, end, start=0, step=1):
        self.end = end
        self.start = start
        self.step = step
        if isinstance(start, (InputParameter, OutputParameter)):
            start = "sprig.atoi(%s)" % start.expr
        if isinstance(step, (InputParameter, OutputParameter)):
            step = "sprig.atoi(%s)" % step.expr
        if isinstance(end, (InputParameter, OutputParameter)):
            end = "sprig.atoi(%s)" % end.expr
        super().__init__("toJson(sprig.untilStep(%s, %s, %s))" %
                         (start, end, step))


def argo_range(
        *args,
) -> ArgoVar:
    """
    Return a str representing a range of integer in Argo
    It receives 1-3 arguments, which is similar to the function `range` in
        Python
    Each argument can be Argo parameter
    """
    if config["mode"] == "debug":
        return Expression("list(range(%s))" % ", ".join(
            map(lambda x: "int(%s)" % to_expr(x), args)))
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
    return ArgoRange(end, start, step)


class ArgoSequence:
    def __init__(self, count, start, end, format):
        self.count = count
        self.start = start
        self.end = end
        self.format = format

    @classmethod
    def from_dict(cls, d):
        if d is None:
            return None
        kwargs = {
            "count": d.get("count", None),
            "start": d.get("start", None),
            "end": d.get("end", None),
            "format": d.get("format", None),
        }
        return cls(**kwargs)

    def convert_to_argo(self):
        count = self.count
        start = self.start
        end = self.end
        if isinstance(count, ArgoVar):
            count = "{{=%s}}" % count.expr
        if isinstance(start, ArgoVar):
            start = "{{=%s}}" % start.expr
        if isinstance(end, ArgoVar):
            end = "{{=%s}}" % end.expr
        return V1alpha1Sequence(count=count, start=start, end=end,
                                format=self.format)


def argo_sequence(
        count: Union[int, ArgoVar] = None,
        start: Union[int, ArgoVar] = None,
        end: Union[int, ArgoVar] = None,
        format: Optional[str] = None,
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
    return ArgoSequence(count=count, start=start, end=end, format=format)


class ArgoLen(ArgoVar):
    def __init__(self, param):
        self.param = param
        if isinstance(param, S3Artifact):
            try:
                path_list = catalog_of_artifact(param)
                if path_list:
                    param.path_list = path_list
            except Exception:
                pass
            super().__init__(str(len(param.path_list)))
        if isinstance(param, InputArtifact):
            assert config["save_path_as_parameter"]
            super().__init__("len(sprig.fromJson(%s))" %
                             param.get_path_list_parameter())
        elif isinstance(param, OutputArtifact):
            assert config["save_path_as_parameter"]
            super().__init__("len(sprig.fromJson(%s))" %
                             param.get_path_list_parameter())
        else:
            if isinstance(param, OutputParameter) and param.save_as_artifact:
                step = param.step
                step.template = step.template.copy()
                step.template.outputs.parameters[param.name].save_both = True
                param.save_both = True
                super().__init__("len(sprig.fromJson(%s))" %
                                 param.expr_as_parameter())
            else:
                super().__init__("len(sprig.fromJson(%s))" % param.expr)


def argo_len(
        param: Union[ArgoVar, S3Artifact],
) -> ArgoVar:
    """
    Return the length of a list which is an Argo parameter

    Args:
        param: the Argo parameter which is a list
    """
    if config["mode"] == "debug":
        return Expression("len(%s)" % to_expr(param))
    return ArgoLen(param)


class ArgoSum:
    def __init__(self, param):
        self.param = param
        self.expr = "sum(%s)" % param


def argo_sum(
        param: ArgoVar,
) -> ArgoSum:
    """
    Return the sum of a list of integers which is an Argo parameter

    Args:
        param: the Argo parameter which is a list of integers
    """
    return ArgoSum(param)


class ArgoConcat:
    def __init__(self, param):
        self.param = param
        self.expr = "concat(%s)" % param


def argo_concat(
        param: ArgoVar,
) -> ArgoConcat:
    """
    Return the concatenation of a list of lists which is an Argo parameter

    Args:
        param: the Argo parameter which is a list of lists
    """
    return ArgoConcat(param)


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
        continue_on_error: continue if the step meets error
        continue_on_num_success: continue if the success number of the
            generated parallel steps greater than certain number
        continue_on_success_ratio: continue if the success ratio of the
            generated parallel steps greater than certain number
        with_sequence: generate parallel steps with respect to a sequence
        key: the key of the step
        executor: define the executor to execute the script
        use_resource: use k8s resource
        util_image: image for utility step
        util_image_pull_policy: image pull policy for utility step
        util_command: command for utility step
        parallelism: parallelism for sliced step
        slices: override slices of OP template
    """

    def __init__(
            self,
            name: str,
            template: OPTemplate,
            parameters: Dict[str, Any] = None,
            artifacts: Dict[str, Union[S3Artifact,
                                       InputArtifact, OutputArtifact]] = None,
            when: Optional[str] = None,
            with_param: Union[str, list,
                              InputParameter, OutputParameter] = None,
            continue_on_failed: bool = False,
            continue_on_error: bool = False,
            continue_on_num_success: Optional[int] = None,
            continue_on_success_ratio: Optional[float] = None,
            with_sequence: Optional[V1alpha1Sequence] = None,
            key: Optional[str] = None,
            executor: Optional[Executor] = None,
            use_resource: Optional[Resource] = None,
            util_image: Optional[str] = None,
            util_image_pull_policy: Optional[str] = None,
            util_command: Union[str, List[str]] = None,
            parallelism: Optional[int] = None,
            slices: Optional[Slices] = None,
    ) -> None:
        self.name = name
        self.id = self.name
        self.template = template
        self.inputs = deepcopy(self.template.inputs)
        self.outputs = deepcopy(self.template.outputs)
        self.inputs.set_step(self)
        self.outputs.set_step(self)
        self.continue_on_failed = continue_on_failed
        self.continue_on_error = continue_on_error
        self.continue_on_num_success = continue_on_num_success
        self.continue_on_success_ratio = continue_on_success_ratio
        self.check_step = None
        self.prepare_step = None
        self.input_artifact_slices = {}

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
        if util_image_pull_policy is None:
            util_image_pull_policy = config["util_image_pull_policy"]
        self.util_image_pull_policy = util_image_pull_policy
        if isinstance(util_command, str):
            util_command = [util_command]
        self.util_command = util_command
        self.parallelism = parallelism

        if hasattr(self.template, "python_packages") and \
                self.template.python_packages:
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

        if slices is not None:
            self.template = self.template.copy()
            self.template.slices = slices
            for name, par in self.template.inputs.parameters.items():
                if name not in self.inputs.parameters:
                    self.inputs.parameters[name] = deepcopy(par)

            self.template.inputs.parameters["dflow_slice"] = InputParameter(
                value=slices.slices)
            self.inputs.parameters["dflow_slice"] = InputParameter(
                value=slices.slices)
            for name in slices.input_parameter:
                for step in (self.template if hasattr(
                        self.template, "__iter__") else []):
                    for par in list(step.inputs.parameters.values()):
                        # input parameter referring to sliced input parameter
                        if par.value is self.template.inputs.parameters[name]:
                            step.template.inputs.parameters["dflow_slice"] = \
                                InputParameter()
                            step.template.add_slices(Slices(
                                "{{inputs.parameters.dflow_slice}}",
                                input_parameter=[par.name],
                                sub_path=slices.sub_path,
                                pool_size=slices.pool_size))
                            step.template.render_script()
                            step.inputs.parameters["dflow_slice"] = \
                                InputParameter(
                                    value="{{inputs.parameters.dflow_slice}}")

            for name in slices.input_artifact:
                for step in (self.template if hasattr(
                        self.template, "__iter__") else []):
                    for art in list(step.inputs.artifacts.values()):
                        # input artifact referring to sliced input artifact
                        if art.source is self.template.inputs.artifacts[name]:
                            if name in self.input_artifact_slices:
                                slice = self.input_artifact_slices[name]
                                pattern = add_prefix_to_slices(
                                    slice, "{{inputs.parameters.dflow_slice}}")
                            else:
                                pattern = "{{inputs.parameters.dflow_slice}}"
                            step.template.inputs.parameters["dflow_slice"] = \
                                InputParameter()
                            step.template.add_slices(Slices(
                                pattern,
                                input_artifact=[art.name],
                                sub_path=slices.sub_path,
                                pool_size=slices.pool_size))
                            step.template.render_script()
                            step.inputs.parameters["dflow_slice"] = \
                                InputParameter(
                                    value="{{inputs.parameters.dflow_slice}}")

            def stack_output_parameter(par):
                if isinstance(par, OutputParameter):
                    step = par.step
                    step.template.inputs.parameters["dflow_slice"] = \
                        InputParameter()
                    step.template.add_slices(Slices(
                        "{{inputs.parameters.dflow_slice}}",
                        output_parameter=[par.name],
                        sub_path=slices.sub_path,
                        pool_size=slices.pool_size))
                    step.template.render_script()
                    step.inputs.parameters["dflow_slice"] = \
                        InputParameter(
                            value="{{inputs.parameters.dflow_slice}}")

            for name in slices.output_parameter:
                # sliced output parameter from
                if self.template.outputs.parameters[name].value_from_parameter\
                        is not None:
                    stack_output_parameter(self.template.outputs.parameters[
                        name].value_from_parameter)
                elif self.template.outputs.parameters[name].\
                        value_from_expression is not None:
                    stack_output_parameter(self.template.outputs.parameters[
                        name].value_from_expression._then)
                    stack_output_parameter(self.template.outputs.parameters[
                        name].value_from_expression._else)

            def stack_output_artifact(art):
                if isinstance(art, OutputArtifact):
                    step = art.step
                    step.template.inputs.parameters["dflow_slice"] = \
                        InputParameter()
                    step.template.add_slices(Slices(
                        "{{inputs.parameters.dflow_slice}}",
                        output_artifact=[art.name],
                        sub_path=slices.sub_path,
                        pool_size=slices.pool_size))
                    step.template.render_script()
                    step.inputs.parameters["dflow_slice"] = \
                        InputParameter(
                            value="{{inputs.parameters.dflow_slice}}")

            for name in slices.output_artifact:
                # sliced output artifact from
                if self.template.outputs.artifacts[name]._from is not None:
                    stack_output_artifact(
                        self.template.outputs.artifacts[name]._from)
                elif self.template.outputs.artifacts[name].from_expression is \
                        not None:
                    stack_output_artifact(self.template.outputs.artifacts[
                        name].from_expression._then)
                    stack_output_artifact(self.template.outputs.artifacts[
                        name].from_expression._else)

        sum_var = None
        if isinstance(self.with_param, ArgoRange) and \
                isinstance(self.with_param.end, ArgoSum):
            sum_var = self.with_param.end.param

        if self.with_sequence is not None and \
                isinstance(self.with_sequence.count, ArgoSum):
            sum_var = self.with_sequence.count.param

        concat_var = None
        if isinstance(self.with_param, ArgoRange) and \
                isinstance(self.with_param.end, ArgoLen) and \
                isinstance(self.with_param.end.param, ArgoConcat):
            concat_var = self.with_param.end.param.param

        if self.with_sequence is not None and \
                isinstance(self.with_sequence.count, ArgoLen) and \
                isinstance(self.with_sequence.count.param, ArgoConcat):
            concat_var = self.with_sequence.count.param.param

        if hasattr(self.template, "slices") and self.template.slices is not \
                None and self.template.slices.group_size is not None:
            self.template = self.template.copy()
            group_size = self.template.slices.group_size
            self.template.inputs.parameters["dflow_nslices"] = InputParameter()
            if self.with_param is not None:
                self.template.inputs.parameters["dflow_with_param"] = \
                    InputParameter()
                self.inputs.parameters["dflow_with_param"] = \
                    InputParameter(value=self.with_param)
                if hasattr(self.with_param, "__len__"):
                    nslices = len(self.with_param)
                else:
                    nslices = argo_len(self.with_param)
                old_slices = self.template.slices.slices
                self.template.slices.slices = \
                    "[json.loads(r'''{{inputs.parameters.dflow_with_param}}"\
                    "''')[%s] for i in range({{item}}*%s, min(({{item}}+1)*%s"\
                    ", {{inputs.parameters.dflow_nslices}}))]" % (
                        old_slices.replace("{{item}}", "i"), group_size,
                        group_size)
                # re-render the script
                self.template.slices = self.template.slices
                for k, slice in self.input_artifact_slices.items():
                    if k in self.template.input_artifact_slices:
                        old = self.template.input_artifact_slices[k]
                        self.template.input_artifact_slices[k] = \
                            add_prefix_to_slices(slice, old)
                        self.template.render_script()
                self.with_param = argo_range(if_expression(
                    "%s %% %s > 0" % (nslices, group_size),
                    "%s/%s + 1" % (nslices, group_size),
                    "%s/%s" % (nslices, group_size)))
            if self.with_sequence is not None:
                self.template.inputs.parameters["dflow_sequence_start"] = \
                    InputParameter()
                self.template.inputs.parameters["dflow_sequence_end"] = \
                    InputParameter()
                self.template.inputs.parameters["dflow_sequence_count"] = \
                    InputParameter()
                start = self.with_sequence.start
                if start is None:
                    start = 0
                end = self.with_sequence.end
                count = self.with_sequence.count
                format = self.with_sequence.format
                self.inputs.parameters["dflow_sequence_start"] = \
                    InputParameter(value=start)
                self.inputs.parameters["dflow_sequence_end"] = \
                    InputParameter(value=end)
                self.inputs.parameters["dflow_sequence_count"] = \
                    InputParameter(value=count)
                if count is not None:
                    nslices = count
                else:
                    nslices = if_expression(
                        "%s > %s" % (end, start),
                        "%s + 1 - %s" % (end, start),
                        "%s + 1 - %s" % (start, end))
                old_slices = self.template.slices.slices
                self.template.slices.slices = \
                    "[[%s for j in (range(int('{{inputs.parameters."\
                    "dflow_sequence_start}}'), int('{{inputs.parameters."\
                    "dflow_sequence_start}}') + int('{{inputs.parameters."\
                    "dflow_sequence_count}}') + 1) if '{{inputs.parameters."\
                    "dflow_sequence_count}}' != 'null' else range(int('{{"\
                    "inputs.parameters.dflow_sequence_start}}'), int('{{"\
                    "inputs.parameters.dflow_sequence_end}}') + 1) if int('{{"\
                    "inputs.parameters.dflow_sequence_end}}') > int('{{"\
                    "inputs.parameters.dflow_sequence_start}}') else range("\
                    "int('{{inputs.parameters.dflow_sequence_start}}'), "\
                    "int('{{inputs.parameters.dflow_sequence_end}}') - 1, -1)"\
                    ")][i] for i in range(int('{{item}}')*%s, min((int('{{"\
                    "item}}')+1)*%s, {{inputs.parameters.dflow_nslices}}))]"\
                    % (old_slices.replace(
                        "'{{item}}'", "('%s' %% j)" % format)
                        if format is not None
                        else old_slices.replace("{{item}}", "j"),
                        group_size, group_size)
                # re-render the script
                self.template.slices = self.template.slices
                for k, slice in self.input_artifact_slices.items():
                    if k in self.template.input_artifact_slices:
                        old = self.template.input_artifact_slices[k]
                        self.template.input_artifact_slices[k] = \
                            add_prefix_to_slices(slice, old)
                        self.template.render_script()
                self.with_sequence = argo_sequence(
                    count=if_expression(
                        "%s %% %s > 0" % (nslices, group_size),
                        "%s/%s + 1" % (nslices, group_size),
                        "%s/%s" % (nslices, group_size)), format=format)

            self.inputs.parameters["dflow_nslices"] = InputParameter(
                value=nslices)

        sliced_output_artifact = self.template.slices.output_artifact if \
            hasattr(self.template, "slices") and \
            self.template.slices is not None else []

        sliced_input_artifact = self.template.slices.input_artifact if \
            hasattr(self.template, "slices") and \
            self.template.slices is not None and \
            self.template.slices.sub_path else []

        if sliced_output_artifact or sliced_input_artifact or \
                sum_var is not None or concat_var is not None:
            self.template = self.template.copy()
            init_template = InitArtifactForSlices(
                self.template, self.util_image, self.util_command,
                self.util_image_pull_policy, self.key, sliced_output_artifact,
                sliced_input_artifact, sum_var, concat_var)
            if self.key is not None:
                self.template.inputs.parameters["dflow_group_key"] = \
                    InputParameter(value="")
                self.inputs.parameters["dflow_group_key"] = InputParameter(
                    value=re.sub("{{=?item.*}}", "group", str(self.key)))
                self.template.inputs.parameters["dflow_artifact_key"] = \
                    InputParameter(value="")
                # For the case of reusing sliced steps, ensure that the output
                # artifacts are reused
                for name in sliced_output_artifact:
                    self.template.outputs.artifacts[name].save.append(
                        S3Artifact(key="{{inputs.parameters."
                                   "dflow_artifact_key}}/%s" % name))

                    def merge_output_artifact(art):
                        step = art.step
                        template = step.template
                        template.inputs.parameters["dflow_group_key"] = \
                            InputParameter()
                        step.inputs.parameters["dflow_group_key"] = \
                            InputParameter(
                                value="{{inputs.parameters.dflow_group_key}}")
                        template.inputs.parameters["dflow_artifact_key"] = \
                            InputParameter()
                        step.inputs.parameters["dflow_artifact_key"] = \
                            InputParameter(value="{{inputs.parameters."
                                           "dflow_artifact_key}}")
                        template.outputs.artifacts[art.name].save.append(
                            S3Artifact(key="{{inputs.parameters."
                                       "dflow_artifact_key}}/%s" % name))

                    if self.template.outputs.artifacts[name]._from is not \
                            None:
                        merge_output_artifact(
                            self.template.outputs.artifacts[name]._from)
                    elif self.template.outputs.artifacts[name].\
                            from_expression is not None:
                        merge_output_artifact(self.template.outputs.artifacts[
                            name].from_expression._then)
                        merge_output_artifact(self.template.outputs.artifacts[
                            name].from_expression._else)
            else:
                self.template.inputs.parameters["dflow_artifact_key"] = \
                    InputParameter(value="")
                for name in sliced_output_artifact:
                    self.template.outputs.artifacts[name].save.append(
                        S3Artifact(key="{{inputs.parameters."
                                   "dflow_artifact_key}}/%s" % name))

                    def merge_output_artifact(art):
                        step = art.step
                        template = step.template
                        template.inputs.parameters["dflow_artifact_key"] = \
                            InputParameter()
                        step.inputs.parameters["dflow_artifact_key"] = \
                            InputParameter(value="{{inputs.parameters."
                                           "dflow_artifact_key}}")
                        template.outputs.artifacts[art.name].save.append(
                            S3Artifact(key="{{inputs.parameters."
                                       "dflow_artifact_key}}/%s" % name))

                    if self.template.outputs.artifacts[name]._from is not \
                            None:
                        merge_output_artifact(
                            self.template.outputs.artifacts[name]._from)
                    elif self.template.outputs.artifacts[name].\
                            from_expression is not None:
                        merge_output_artifact(self.template.outputs.artifacts[
                            name].from_expression._then)
                        merge_output_artifact(self.template.outputs.artifacts[
                            name].from_expression._else)

            if self.key is not None:
                group_key = re.sub("{{=?item.*}}", "group", str(self.key))
                self.prepare_step = self.__class__(
                    name="%s-init-artifact" % self.name,
                    template=init_template,
                    parameters={"dflow_group_key": group_key})
                self.inputs.parameters["dflow_artifact_key"] = InputParameter(
                    value=self.prepare_step.outputs.parameters[
                        "dflow_artifact_key"])
            else:
                self.prepare_step = self.__class__(
                    name="%s-init-artifact" % self.name,
                    template=init_template)
                self.inputs.parameters["dflow_artifact_key"] = InputParameter(
                    value=self.prepare_step.outputs.parameters[
                        "dflow_artifact_key"])

            if sliced_input_artifact:
                if self.key is not None:
                    self.prepare_step.inputs.parameters["dflow_key"].value = \
                        "%s-init-artifact" % group_key
                for name in sliced_input_artifact:
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
                    elif isinstance(v, (InputArtifact, OutputArtifact,
                                        LocalArtifact)):
                        self.prepare_step.inputs.artifacts[name].source = v
                        self.inputs.artifacts[name].sub_path = \
                            "{{item.%s}}" % name
                self.with_param = self.prepare_step.outputs.parameters[
                    "dflow_slices_path"]

            for name in sliced_output_artifact:
                self.outputs.artifacts[name].redirect = \
                    self.prepare_step.outputs.artifacts[name]

            if isinstance(self.with_param, ArgoRange) and \
                    isinstance(self.with_param.end, ArgoSum):
                name = sum_var.name
                self.prepare_step.inputs.parameters[name] = InputParameter(
                    value=str(sum_var))
                self.with_param = ArgoRange(
                    self.prepare_step.outputs.parameters["sum_%s" % name],
                    self.with_param.start,
                    self.with_param.step)

            if self.with_sequence is not None and \
                    isinstance(self.with_sequence.count, ArgoSum):
                name = sum_var.name
                self.prepare_step.inputs.parameters[name] = InputParameter(
                    value=str(sum_var))
                self.with_sequence = argo_sequence(
                    self.prepare_step.outputs.parameters["sum_%s" % name],
                    self.with_sequence.start, self.with_sequence.end,
                    self.with_sequence.format)

            if isinstance(self.with_param, ArgoRange) and \
                    isinstance(self.with_param.end, ArgoLen) and \
                    isinstance(self.with_param.end.param, ArgoConcat):
                name = concat_var.name
                self.prepare_step.inputs.parameters[name] = InputParameter(
                    value=str(concat_var))
                self.with_param = ArgoRange(
                    argo_len(self.prepare_step.outputs.parameters[
                        "concat_%s" % name]),
                    self.with_param.start,
                    self.with_param.step)

            if self.with_sequence is not None and \
                    isinstance(self.with_sequence.count, ArgoLen) and \
                    isinstance(self.with_sequence.count.param, ArgoConcat):
                name = concat_var.name
                self.prepare_step.inputs.parameters[name] = InputParameter(
                    value=str(concat_var))
                self.with_sequence = argo_sequence(
                    argo_len(self.prepare_step.outputs.parameters[
                        "concat_%s" % name]),
                    self.with_sequence.start, self.with_sequence.end,
                    self.with_sequence.format)

        if config["register_tasks"] and hasattr(self.template, "slices") and \
                self.template.slices and \
                self.template.slices.register_first_only:
            self.template = self.template.copy()
            if self.with_param is not None:
                if isinstance(self.with_param, ArgoVar):
                    par = self.with_param.expr
                else:
                    par = self.with_param
                first = "jsonpath(%s, '$[0]')" % par
                self.inputs.parameters["dflow_first"] = InputParameter(
                    value="{{=string(%s) == %s ? %s : toJson(%s)}}" %
                    (first, first, first, first))
            elif self.with_sequence is not None:
                if self.with_sequence.start is not None:
                    first = self.with_sequence.start
                else:
                    first = 0
                self.inputs.parameters["dflow_first"] = InputParameter(
                    value=first)
                if self.with_sequence.format is not None:
                    self.template.first_var = "'" + self.with_sequence.format \
                        + "' % {{inputs.parameters.dflow_first}}"
                    self.template.render_script()

        pvc_arts = []
        for art in self.inputs.artifacts.values():
            if isinstance(art.source, PVC):
                pvc_arts.append((art.source, art))

        if len(pvc_arts) > 0:
            self.template = self.template.copy()
            if (isinstance(self.template, ShellOPTemplate)):
                for pvc, art in pvc_arts:
                    del self.template.inputs.artifacts[art.name]
                    self.template.script = "ln -s /tmp/mnt/%s %s\n" % (
                        pvc.subpath, art.path) + self.template.script
                    self.template.mounts.append(V1VolumeMount(
                        name=pvc.name, mount_path="/tmp/mnt"))
                    self.template.pvcs.append(pvc)
            elif (isinstance(self.template, PythonScriptOPTemplate)):
                for pvc, art in pvc_arts:
                    del self.template.inputs.artifacts[art.name]
                    self.template.script = \
                        "os.system('ln -s /tmp/mnt/%s %s')\n" % (
                            pvc.subpath, art.path) + self.template.script
                    self.template.mounts.append(V1VolumeMount(
                        name=pvc.name, mount_path="/tmp/mnt"))
                    self.template.pvcs.append(pvc)
                self.template.script = "import os\n" + self.template.script
            else:
                raise RuntimeError(
                    "Unsupported type of OPTemplate to mount PVC")

        pvc_arts = []
        for art in self.outputs.artifacts.values():
            for save in art.save:
                if isinstance(save, PVC):
                    pvc_arts.append((save, art))

        if len(pvc_arts) > 0:
            self.template = self.template.copy()
            if (isinstance(self.template, ShellOPTemplate)):
                self.template.script += "\n"
                for pvc, art in pvc_arts:
                    self.template.mounts.append(V1VolumeMount(
                        name=pvc.name, mount_path="/tmp/mnt"))
                    self.template.script += "cp -r %s /tmp/mnt/%s\n" % (
                        art.path, pvc.subpath)
                    self.template.pvcs.append(pvc)
            elif (isinstance(self.template, PythonScriptOPTemplate)):
                self.template.script += "\nimport os\n"
                for pvc, art in pvc_arts:
                    self.template.mounts.append(V1VolumeMount(
                        name=pvc.name, mount_path="/tmp/mnt"))
                    self.template.script += \
                        "os.system('cp -r %s /tmp/mnt/%s')\n" % (
                            art.path, pvc.subpath)
                    self.template.pvcs.append(pvc)
            else:
                raise RuntimeError(
                    "Unsupported type of OPTemplate to mount PVC")

        if self.continue_on_num_success or self.continue_on_success_ratio is \
                not None:
            self.continue_on_failed = True
            self.template = self.template.copy()
            from .steps import Steps
            if isinstance(self.template, ScriptOPTemplate):
                self.template.outputs.parameters["dflow_success_tag"] = \
                    OutputParameter(value_from_path="/tmp/outputs/success_tag",
                                    default="0")
                self.outputs.parameters["dflow_success_tag"] = \
                    OutputParameter(value_from_path="/tmp/outputs/success_tag",
                                    default="0")
                from .python import PythonOPTemplate
                if isinstance(self.template, PythonOPTemplate):
                    self.template.post_script += "\nwith open('{tmp_root}"\
                        "/outputs/success_tag', 'w') as f:\n    f.write('1')\n"
                    self.template.render_script()
                elif isinstance(self.template, ShellOPTemplate):
                    self.template.script += "\necho 1 > /tmp/outputs"\
                        "/success_tag\n"
                elif isinstance(self.template, PythonScriptOPTemplate):
                    self.template.script += "\nwith open('/tmp/outputs"\
                        "/success_tag', 'w') as f:\n    f.write('1')\n"
            elif isinstance(self.template, Steps):
                last_step = self.template.steps[-1]
                last_templ = last_step.template
                last_templ.outputs.parameters["dflow_success_tag"] = \
                    OutputParameter(value_from_path="/tmp/outputs/success_tag",
                                    default="0")
                last_step.outputs.parameters["dflow_success_tag"] = \
                    OutputParameter(value_from_path="/tmp/outputs/success_tag",
                                    default="0")
                from .python import PythonOPTemplate
                if isinstance(last_templ, PythonOPTemplate):
                    last_templ.post_script += "\nwith open('{tmp_root}"\
                        "/outputs/success_tag', 'w') as f:\n    f.write('1')\n"
                    last_templ.render_script()
                elif isinstance(last_templ, ShellOPTemplate):
                    last_templ.script += "\necho 1 > /tmp/outputs"\
                        "/success_tag\n"
                elif isinstance(last_templ, PythonScriptOPTemplate):
                    last_templ.script += "\nwith open('/tmp/outputs"\
                        "/success_tag', 'w') as f:\n    f.write('1')\n"
                else:
                    raise RuntimeError(
                        "Unsupported type of OPTemplate for "
                        "continue_on_num_success or continue_on_success_ratio")
                self.template.outputs.parameters["dflow_success_tag"] = \
                    OutputParameter(value_from_parameter=last_step.outputs.
                                    parameters["dflow_success_tag"],
                                    default="0")
                self.outputs.parameters["dflow_success_tag"] = \
                    OutputParameter(value_from_parameter=last_step.outputs.
                                    parameters["dflow_success_tag"],
                                    default="0")
            else:
                raise RuntimeError(
                    "Unsupported type of OPTemplate for "
                    "continue_on_num_success or continue_on_success_ratio")

        if self.continue_on_num_success is not None:
            self.check_step = self.__class__(
                name="%s-check-num-success" % self.name,
                template=CheckNumSuccess(
                    name="%s-check-num-success" % self.template.name,
                    image=self.util_image,
                    image_pull_policy=self.util_image_pull_policy),
                parameters={
                    "success": self.outputs.parameters["dflow_success_tag"],
                    "threshold": self.continue_on_num_success
                }
            )
        elif self.continue_on_success_ratio is not None:
            if self.with_param is not None:
                if hasattr(self.with_param, "__len__"):
                    total = len(self.with_param)
                else:
                    total = argo_len(self.with_param)
            elif self.with_sequence is not None:
                if self.with_sequence.count is not None:
                    count = self.with_sequence.count
                    total = count
                start = 0
                if self.with_sequence.start is not None:
                    start = self.with_sequence.start
                if self.with_sequence.end is not None:
                    end = self.with_sequence.end
                    total = if_expression(
                        "%s > %s" % (end, start),
                        "%s + 1 - %s" % (end, start),
                        "%s + 1 - %s" % (start, end))
            self.check_step = self.__class__(
                name="%s-check-success-ratio" % self.name,
                template=CheckSuccessRatio(
                    name="%s-check-success-ratio" % self.template.name,
                    image=self.util_image,
                    image_pull_policy=self.util_image_pull_policy),
                parameters={
                    "success": self.outputs.parameters["dflow_success_tag"],
                    "total": total,
                    "threshold": self.continue_on_success_ratio
                }
            )

        if self.parallelism is not None:
            assert self.with_param is not None or self.with_sequence is not \
                None, "Only parallel step can be assigned with parallelism"
            from .dag import DAG
            from .steps import Steps
            from .task import Task
            if isinstance(self, Task):
                steps = DAG(name="%s-par-steps" % self.template.name,
                            parallelism=self.parallelism)
            else:
                steps = Steps(name="%s-par-steps" % self.template.name,
                              parallelism=self.parallelism)
            steps.inputs = deepcopy(self.template.inputs)
            for par in steps.inputs.parameters.values():
                par.path = None
            for art in steps.inputs.artifacts.values():
                art.path = None
            steps.outputs = deepcopy(self.template.outputs)
            for par in steps.outputs.parameters.values():
                par.value_from_path = None
            for art in steps.outputs.artifacts.values():
                art.path = None

            step = deepcopy(self)
            step.when = None
            for name in list(self.inputs.parameters.keys()):
                if name[:6] == "dflow_" and name[-9:] == "_sub_path" or \
                        name[:10] == "dflow_var_":
                    del steps.inputs.parameters[name]
                    del self.inputs.parameters[name]
                else:
                    step.set_parameters({name: steps.inputs.parameters[name]})
            for name, art in list(self.inputs.artifacts.items()):
                art.sub_path = None
                if isinstance(art.source, S3Artifact):
                    del steps.inputs.artifacts[name]
                    del self.inputs.artifacts[name]
                else:
                    step.set_artifacts({name: steps.inputs.artifacts[name]})
            if self.prepare_step is not None:
                for name in list(self.prepare_step.inputs.parameters.keys()):
                    step.prepare_step.set_parameters({
                        name: steps.inputs.parameters[name]})
                for name, art in list(
                        self.prepare_step.inputs.artifacts.items()):
                    if not isinstance(art.source, S3Artifact):
                        step.prepare_step.set_artifacts({
                            name: steps.inputs.artifacts[name]})
            steps.add(step)
            for name, par in list(self.outputs.parameters.items()):
                if not par.save_as_artifact:
                    steps.outputs.parameters[name].value_from_parameter = \
                        step.outputs.parameters[name]
                else:
                    del steps.outputs.parameters[name]
                    del self.outputs.parameters[name]
            for name, art in list(self.outputs.artifacts.items()):
                if art.redirect is not None:
                    steps.outputs.artifacts[name]._from = \
                        step.outputs.artifacts[name]
                else:
                    del steps.outputs.artifacts[name]
                    del self.outputs.artifacts[name]

            for name in self.outputs.artifacts.keys():
                self.outputs.artifacts[name].redirect = None
            self.template = steps
            self.continue_on_num_success = None
            self.continue_on_success_ratio = None
            self.key = None
            self.executor = None
            self.use_resource = None
            self.prepare_step = None
            self.check_step = None

            if "dflow_key" in steps.inputs.parameters:
                fields = re.split("{{item[^}]*}}",
                                  self.inputs.parameters["dflow_key"].value)
                exprs = re.findall("{{item[^}]*}}",
                                   self.inputs.parameters["dflow_key"].value)
                for i in range(len(fields)):
                    steps.inputs.parameters["dflow_key_%s" % i] = \
                        InputParameter()
                del steps.inputs.parameters["dflow_key"]
                key = str(steps.inputs.parameters["dflow_key_0"])
                for i, expr in enumerate(exprs):
                    key += expr
                    key += str(steps.inputs.parameters["dflow_key_%s" % (i+1)])
                step.set_parameters({"dflow_key": key})
                for i, field in enumerate(fields):
                    self.inputs.parameters["dflow_key_%s" % i] = \
                        InputParameter(value=field)
                del self.inputs.parameters["dflow_key"]
            if self.with_param is not None:
                steps.inputs.parameters["dflow_with_param"] = InputParameter()
                step.with_param = steps.inputs.parameters["dflow_with_param"]
                self.inputs.parameters["dflow_with_param"] = InputParameter(
                    value=self.with_param)
                self.with_param = None
            if self.with_sequence is not None:
                if self.with_sequence.start is not None:
                    steps.inputs.parameters["dflow_sequence_start"] = \
                        InputParameter()
                    step.with_sequence.start = steps.inputs.parameters[
                        "dflow_sequence_start"]
                    self.inputs.parameters["dflow_sequence_start"] = \
                        InputParameter(value=self.with_sequence.start)
                if self.with_sequence.end is not None:
                    steps.inputs.parameters["dflow_sequence_end"] = \
                        InputParameter()
                    step.with_sequence.end = steps.inputs.parameters[
                        "dflow_sequence_end"]
                    self.inputs.parameters["dflow_sequence_end"] = \
                        InputParameter(value=self.with_sequence.end)
                if self.with_sequence.count is not None:
                    steps.inputs.parameters["dflow_sequence_count"] = \
                        InputParameter()
                    step.with_sequence.count = steps.inputs.parameters[
                        "dflow_sequence_count"]
                    self.inputs.parameters["dflow_sequence_count"] = \
                        InputParameter(value=self.with_sequence.count)
                self.with_sequence = None

        if GLOBAL_CONTEXT.in_context:
            if not self.name.endswith('init-artifact'):
                GLOBAL_CONTEXT.current_workflow.add(self)
            else:
                if self.name.endswith('init-artifact-init-artifact'):
                    raise ValueError(
                        "Please don't name step as '***init-artifact'")

    @classmethod
    def from_dict(cls, d, templates):
        kwargs = {
            "name": d["name"],
            "template": templates[d["template"]],
            "parameters": {par["name"]: par["value"] for par in
                           d.get("arguments", {}).get("parameters", [])},
            "artifacts": {},
            "when": d.get("when", None),
            "with_param": d.get("withParam", None) or d.get("withItems", None),
            "continue_on_failed": d.get("continueOn", {}).get("failed", False),
            "continue_on_error": d.get("continueOn", {}).get("error", False),
            "with_sequence": ArgoSequence.from_dict(d.get("withSequence",
                                                          None)),
            "key": d.get("arguments", {}).get("parameters", {})
        }
        for art in d.get("arguments", {}).get("artifacts", {}):
            name = art["name"]
            if "from" in art:
                kwargs["artifacts"][name] = art["from"]
                if "subPath" in art:
                    kwargs["artifacts"][name] += "/" + art["subPath"]
            elif "s3" in art:
                kwargs["artifacts"][name] = S3Artifact(key=art["s3"]["key"])
                if "subPath" in art:
                    kwargs["artifacts"][name] = kwargs["artifacts"][
                        name].sub_path(art["subPath"])
            elif "oss" in art:
                kwargs["artifacts"][name] = S3Artifact(
                    key=art["oss"]["key"]).oss()
                if "subPath" in art:
                    kwargs["artifacts"][name] = kwargs["artifacts"][
                        name].sub_path(art["subPath"])
        kwargs["key"] = kwargs["parameters"].get("dflow_key", None)
        return cls(**kwargs)

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
                    if not v.save_as_artifact and v.step is not None:
                        raise TypeError("%s is big parameter, but %s is not"
                                        " big parameter" % (
                                            self.inputs.parameters[k], v))
                    v.save_as_artifact = True
                if v.save_as_artifact:
                    if not self.inputs.parameters[k].save_as_artifact and \
                            v.step is not None:
                        raise TypeError("%s is big parameter, but %s is not"
                                        " big parameter" % (
                                            v, self.inputs.parameters[k]))
                    self.inputs.parameters[k].save_as_artifact = True

            if self.inputs.parameters[k].save_as_artifact and isinstance(v, (
                    InputParameter, OutputParameter, InputArtifact,
                    OutputArtifact)):
                self.inputs.parameters[k].source = v
                continue

            self.inputs.parameters[k].value = v

    def set_artifacts(self, artifacts):
        for k, v in artifacts.items():
            if v is None:
                del self.inputs.artifacts[k]
                self.template.inputs.artifacts[k].optional = True
            elif isinstance(v, (list, tuple)):
                self.template = self.template.copy()
                slices = []
                for i, a in enumerate(v):
                    vn = "dflow_%s_%s" % (k, i)
                    self.template.inputs.artifacts[vn] = deepcopy(
                        self.template.inputs.artifacts[k])
                    self.inputs.artifacts[vn] = deepcopy(
                        self.template.inputs.artifacts[vn])
                    self.inputs.artifacts[vn].source = a
                    if hasattr(a, "slice") and a.slice is not None:
                        slices.append(a.slice)
                    else:
                        slices.append(None)
                from .steps import Steps
                from .dag import DAG
                if isinstance(self.template, (Steps, DAG)):
                    for step in self.template:
                        for name, art in list(step.inputs.artifacts.items()):
                            if art.source is self.template.inputs.artifacts[k]:
                                step.set_artifacts({name: [
                                    self.template.inputs.artifacts[
                                        "dflow_%s_%s" % (k, i)]
                                    if slice is None else
                                    self.template.inputs.artifacts[
                                        "dflow_%s_%s" % (k, i)][slice]
                                    for i, slice in enumerate(slices)]})
                else:
                    for i, a in enumerate(v):
                        vn = "dflow_%s_%s" % (k, i)
                        self.template.inputs.artifacts[vn].path = \
                            "%s/inputs/artifacts/%s" % (self.template.tmp_root,
                                                        vn)
                    if any(map(lambda x: x is not None, slices)):
                        self.template.input_artifact_slices[k] = slices
                    self.template.n_parts[k] = len(v)
                    self.template.render_script()
                del self.template.inputs.artifacts[k]
                del self.inputs.artifacts[k]
            else:
                self.inputs.artifacts[k].source = v
                if hasattr(v, "slice") and v.slice is not None:
                    self.input_artifact_slices[k] = v.slice
                    self.template = self.template.copy()
                    if isinstance(v.slice, (InputParameter, OutputParameter)):
                        self.template.inputs.parameters[
                            "dflow_%s" % v.slice.name] = InputParameter()
                        self.inputs.parameters["dflow_%s" % v.slice.name] = \
                            InputParameter(value=v.slice)
                        v.slice = "{{inputs.parameters.dflow_%s}}" % \
                            v.slice.name
                    from .steps import Steps
                    from .dag import DAG
                    if isinstance(self.template, (Steps, DAG)):
                        for step in self.template:
                            for name, art in step.inputs.artifacts.items():
                                if art.source is \
                                        self.template.inputs.artifacts[k] or \
                                        hasattr(art.source, "parent") and \
                                        art.source.parent is \
                                        self.template.inputs.artifacts[k]:
                                    if hasattr(art.source, "slice") and \
                                            art.source.slice is not None:
                                        slice = "%s.%s" % (v.slice,
                                                           art.source.slice)
                                    else:
                                        slice = v.slice
                                    step.set_artifacts({
                                        name: self.template.inputs.artifacts[
                                            k][slice]})
                    else:
                        if k in self.template.input_artifact_slices:
                            old = self.template.input_artifact_slices[k]
                            self.template.input_artifact_slices[k] = \
                                add_prefix_to_slices(v.slice, old)
                        else:
                            self.template.input_artifact_slices[k] = v.slice \
                                if isinstance(v.slice, int) \
                                else "'%s'" % v.slice
                        self.template.render_script()
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
                if config["register_tasks"] and k[:6] != "dflow_":
                    if isinstance(v, S3Artifact):
                        self.inputs.parameters["dflow_%s_urn" % k] = \
                            InputParameter(value=v.urn)
                    elif isinstance(v, OutputArtifact) and v.step is not None \
                            and "dflow_%s_urn" % v.name in \
                                v.step.outputs.parameters:
                        self.inputs.parameters["dflow_%s_urn" % k] = \
                            InputParameter(
                            value=v.step.outputs.parameters[
                                "dflow_%s_urn" % v.name])
                    elif isinstance(v, InputArtifact) and v.template is not \
                        None and "dflow_%s_urn" % v.name in \
                            v.template.inputs.parameters:
                        self.inputs.parameters["dflow_%s_urn" % k] = \
                            InputParameter(
                            value=v.template.inputs.parameters[
                                "dflow_%s_urn" % v.name])

    def render_by_executor(self, context=None):
        if self.executor is not None:
            assert isinstance(self.executor, Executor)
            self.template = self.executor.render(self.template)
            if hasattr(self.executor, "merge_sliced_step") and \
                    self.executor.merge_sliced_step:
                self.inputs.parameters["dflow_with_param"] = \
                    InputParameter(value="")
                self.inputs.parameters["dflow_sequence_start"] = \
                    InputParameter(value=0)
                self.inputs.parameters["dflow_sequence_end"] = \
                    InputParameter(value=None)
                self.inputs.parameters["dflow_sequence_count"] = \
                    InputParameter(value=None)
                self.inputs.parameters["dflow_sequence_format"] = \
                    InputParameter(value="")
                if self.with_param is not None:
                    self.inputs.parameters["dflow_with_param"].value = \
                        self.with_param
                    self.with_param = None
                if self.with_sequence is not None:
                    start = self.with_sequence.start
                    if start is None:
                        start = 0
                    end = self.with_sequence.end
                    count = self.with_sequence.count
                    format = self.with_sequence.format
                    self.inputs.parameters["dflow_sequence_start"].value = \
                        start
                    self.inputs.parameters["dflow_sequence_end"].value = end
                    self.inputs.parameters["dflow_sequence_count"].value = \
                        count
                    self.inputs.parameters["dflow_sequence_format"].value = \
                        format
                    self.with_sequence = None
                if self.key is not None:
                    self.inputs.parameters["dflow_key"] = InputParameter(
                        value=str(self.key).replace("{{item}}", "merged"))
        elif context is not None:
            self.template = context.render(self.template)

    def prepare_argo_arguments(self, context=None):
        if isinstance(self.with_param, ArgoVar):
            self.with_param = "{{=%s}}" % self.with_param.expr
        elif self.with_param is not None and not isinstance(self.with_param,
                                                            str):
            self.with_param = jsonpickle.dumps(list(self.with_param))

        self.render_by_executor(context)

        self.argo_parameters = []
        self.argo_artifacts = []
        for par in self.inputs.parameters.values():
            if par.save_as_artifact:
                self.argo_artifacts.append(par.convert_to_argo())
            else:
                self.argo_parameters.append(par.convert_to_argo())

        for art in self.inputs.artifacts.values():
            if isinstance(art.source, PVC):
                pass
            elif art.source is None and art.optional:
                pass
            else:
                self.argo_artifacts.append(art.convert_to_argo())

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
        if isinstance(self.when, ArgoVar):
            self.when = "{{=%s}}" % self.when.expr
        return V1alpha1WorkflowStep(
            name=self.name, template=self.template.name,
            arguments=V1alpha1Arguments(
                parameters=self.argo_parameters,
                artifacts=self.argo_artifacts
            ), when=self.when, with_param=self.with_param,
            with_sequence=None if self.with_sequence is None else
            self.with_sequence.convert_to_argo(),
            continue_on=V1alpha1ContinueOn(failed=self.continue_on_failed,
                                           error=self.continue_on_error)
        )

    def run(self, scope, context=None):
        self.phase = "Running"
        self.render_by_executor(context)

        import os

        from .dag import DAG
        from .steps import Steps

        if self.when is not None:
            if isinstance(self.when, Expression):
                value = self.when.eval(scope)
            elif isinstance(self.when, (InputParameter, OutputParameter)):
                value = get_var(self.when, scope).value
            elif isinstance(self.when, ArgoVar):
                value = Expression(self.when.expr).eval(scope)
            elif isinstance(self.when, str):
                value = eval_expr(render_expr(self.when, scope))
            if not value:
                self.phase = "Skipped"
                return

        # source input parameters
        parameters = deepcopy(self.inputs.parameters)
        for k, v in self.template.outputs.parameters.items():
            if hasattr(v, "value"):
                self.outputs.parameters[k].value = v.value
        for name, par in parameters.items():
            value = par.value

            def handle_expr(val, scope):
                if isinstance(val, dict):
                    for k, v in val.items():
                        if isinstance(v, Expression):
                            val[k] = v.eval(scope)
                        else:
                            handle_expr(v, scope)
                elif isinstance(val, list):
                    for i, v in enumerate(val):
                        if isinstance(v, Expression):
                            val[i] = v.eval(scope)
                        else:
                            handle_expr(v, scope)
                elif hasattr(val, "__dict__"):
                    for k, v in val.__dict__.items():
                        if isinstance(v, Expression):
                            val.__dict__[k] = v.eval(scope)
                        else:
                            handle_expr(v, scope)

            if isinstance(value, Expression):
                par.value = value.eval(scope)
            elif isinstance(value, (InputParameter, OutputParameter)):
                par.value = get_var(value, scope).value
            elif isinstance(value, ArgoVar):
                par.value = Expression(value.expr).eval(scope)
            elif isinstance(value, str):
                par.value = render_expr(value, scope)
            else:
                try:
                    handle_expr(par.value, scope)
                except Exception as e:
                    logging.warn("Failed to handle expressions in parameter"
                                 " value: ", e)

        # source input artifacts
        for name, art in self.inputs.artifacts.items():
            if isinstance(art.source, (InputArtifact, OutputArtifact)):
                art.source = get_var(art.source, scope)

        if isinstance(self.template, (Steps, DAG)):
            if hasattr(self.template, "orig_template"):
                steps = deepcopy(self.template.orig_template)
                steps.orig_template = self.template.orig_template
            else:
                steps = deepcopy(self.template)
                steps.orig_template = self.template

            # override default inputs with arguments
            for name, par in parameters.items():
                steps.inputs.parameters[name].value = par.value

            for name, art in self.inputs.artifacts.items():
                if isinstance(art.source, S3Artifact) and not hasattr(
                        art.source, "local_path"):
                    path = os.path.abspath("download/%s" % randstr())
                    art.source.download(path=path, debug_download=True,
                                        remove_catalog=False)
                    fs = os.listdir(path)
                    if len(fs) == 1 and os.path.isfile(os.path.join(path,
                                                                    fs[0])):
                        os.rename(os.path.join(path, fs[0]), path + ".bk")
                        os.rmdir(path)
                        os.rename(path + ".bk", path)
                    art.source.local_path = path
                if not hasattr(art.source, "local_path") and art.optional:
                    continue
                steps.inputs.artifacts[name].local_path = art.source.local_path

            if "dflow_key" in steps.inputs.parameters and \
                    steps.inputs.parameters["dflow_key"].value:
                step_id = steps.inputs.parameters["dflow_key"].value
                stepdir = os.path.abspath(step_id)
                if os.path.exists(stepdir):
                    self.load_output_parameters(stepdir,
                                                self.outputs.parameters)
                    self.load_output_artifacts(stepdir,
                                               self.outputs.artifacts)
                    with open(os.path.join(stepdir, "phase"), "r") as f:
                        self.phase = f.read()
                    return
                os.makedirs(stepdir)
            else:
                while True:
                    step_id = self.name + "-" + randstr()
                    stepdir = os.path.abspath(step_id)
                    if not os.path.exists(stepdir):
                        os.makedirs(stepdir)
                        break

            with open(os.path.join(stepdir, "type"), "w") as f:
                if isinstance(self.template, Steps):
                    f.write("Steps")
                elif isinstance(self.template, DAG):
                    f.write("DAG")
            with open(os.path.join(stepdir, "phase"), "w") as f:
                f.write("Running")
            self.record_input_parameters(stepdir, steps.inputs.parameters)
            self.record_input_artifacts(stepdir, steps.inputs.artifacts, None,
                                        scope, True)

            try:
                steps.run(scope.workflow_id)
            except Exception:
                self.phase = "Failed"
                with open(os.path.join(stepdir, "phase"), "w") as f:
                    f.write("Failed")
                raise RuntimeError("Step %s failed" % self)

            for name, par in self.outputs.parameters.items():
                par1 = self.template.outputs.parameters[name]
                if par1.value_from_parameter is not None:
                    par.value = get_var(par1.value_from_parameter, steps).value
                elif par1.value_from_expression is not None:
                    if isinstance(par1.value_from_expression, str):
                        expr = replace_argo_func(par1.value_from_expression)
                        par1.value_from_expression = Expression(expr)
                    par.value = par1.value_from_expression.eval(steps)

            for name, art in self.outputs.artifacts.items():
                art1 = self.template.outputs.artifacts[name]
                if art1._from is not None:
                    art.local_path = get_var(art1._from, steps).local_path
                elif art1.from_expression is not None:
                    if isinstance(art1.from_expression, str):
                        expr = replace_argo_func(art1.from_expression)
                        art1.from_expression = Expression(expr)
                    art.local_path = art1.from_expression.eval(steps)

            self.record_output_parameters(stepdir, self.outputs.parameters)
            self.record_output_artifacts(stepdir, self.outputs.artifacts)
            self.phase = "Succeeded"
            with open(os.path.join(stepdir, "phase"), "w") as f:
                f.write("Succeeded")
            return

        if self.with_param is not None or self.with_sequence is not None:
            if isinstance(self.with_param, Expression):
                item_list = self.with_param.eval(scope)
            elif isinstance(self.with_param, (InputParameter,
                                              OutputParameter)):
                item_list = self.with_param.value
            elif isinstance(self.with_param, list):
                item_list = self.with_param
            elif self.with_sequence is not None:
                start = 0
                if self.with_sequence.start is not None:
                    start = self.with_sequence.start
                    if isinstance(start, Expression):
                        start = int(start.eval(scope))
                    elif isinstance(start, (InputParameter, OutputParameter)):
                        start = start.value
                    elif isinstance(start, ArgoVar):
                        start = int(Expression(start.expr).eval(scope))
                if self.with_sequence.count is not None:
                    count = self.with_sequence.count
                    if isinstance(count, Expression):
                        count = int(count.eval(scope))
                    elif isinstance(count, (InputParameter, OutputParameter)):
                        count = count.value
                    elif isinstance(count, ArgoVar):
                        count = int(Expression(count.expr).eval(scope))
                    sequence = list(range(start, start + count))
                if self.with_sequence.end is not None:
                    end = self.with_sequence.end
                    if isinstance(end, Expression):
                        end = int(end.eval(scope))
                    elif isinstance(end, (InputParameter, OutputParameter)):
                        end = end.value
                    elif isinstance(end, ArgoVar):
                        end = int(Expression(end.expr).eval(scope))
                    if end >= start:
                        sequence = list(range(start, end + 1))
                    else:
                        sequence = list(range(start, end - 1, -1))
                if self.with_sequence.format is not None:
                    item_list = [self.with_sequence.format % i
                                 for i in sequence]
                else:
                    item_list = sequence
            elif isinstance(self.with_param, str):
                self.with_param = render_expr(self.with_param, scope)
                item_list = eval(self.with_param)
            else:
                raise RuntimeError("Not supported")

            procs = []
            self.parallel_steps = []
            assert isinstance(item_list, list)
            from multiprocessing import Process, Queue
            queue = Queue()
            for i, item in enumerate(item_list):
                ps = deepcopy(self)
                ps.phase = "Pending"
                self.parallel_steps.append(ps)
                proc = Process(target=ps.exec_with_queue,
                               args=(scope, parameters, i, queue, item,
                                     config, s3_config))
                proc.start()
                procs.append(proc)

            for i in range(len(item_list)):
                # TODO: if the process is killed, this will be blocked forever
                j, ps = queue.get()
                if ps is None:
                    self.parallel_steps[j].phase = "Failed"
                    if not self.continue_on_failed:
                        self.phase = "Failed"
                        raise RuntimeError("Step %s failed" %
                                           self.parallel_steps[j])
                else:
                    self.parallel_steps[j].outputs = deepcopy(ps.outputs)

            for name, par in self.outputs.parameters.items():
                par.value = []
                for ps in self.parallel_steps:
                    if not hasattr(ps.outputs.parameters[name], "value") and \
                            hasattr(ps.outputs.parameters[name], "default"):
                        value = ps.outputs.parameters[name].default
                    else:
                        value = ps.outputs.parameters[name].value
                    if isinstance(value, str):
                        par.value.append(value)
                    else:
                        par.value.append(jsonpickle.dumps(value))
            for name, art in self.outputs.artifacts.items():
                for save in self.template.outputs.artifacts[name].save:
                    if isinstance(save, S3Artifact):
                        key = render_script(save.key, parameters,
                                            scope.workflow_id)
                        art.local_path = os.path.abspath(os.path.join("..",
                                                                      key))
            self.phase = "Succeeded"
        else:
            try:
                self.exec(scope, parameters)
            except Exception:
                self.phase = "Failed"
                if not self.continue_on_failed:
                    raise RuntimeError("Step %s failed" % self)

    def run_with_queue(self, scope, context, order, queue, conf, s3_conf):
        try:
            config.update(conf)
            s3_config.update(s3_conf)
            self.run(scope, context)
            queue.put((order, self))
        except Exception:
            import traceback
            traceback.print_exc()
            queue.put((order, None))

    def record_input_parameters(self, stepdir, parameters):
        os.makedirs(os.path.join(stepdir, "inputs/parameters"), exist_ok=True)
        for name, par in parameters.items():
            par_path = os.path.join(stepdir, "inputs/parameters/%s" % name)
            with open(par_path, "w") as f:
                f.write(par.value if isinstance(par.value, str)
                        else jsonpickle.dumps(par.value))
            if par.type is not None:
                os.makedirs(os.path.join(
                    stepdir, "inputs/parameters/.dflow"), exist_ok=True)
                with open(os.path.join(
                        stepdir, "inputs/parameters/.dflow/%s" % name),
                        "w") as f:
                    f.write(jsonpickle.dumps({"type": str(par.type)}))

    def record_input_artifacts(self, stepdir, artifacts, item, scope,
                               ignore_nonexist=False):
        os.makedirs(os.path.join(stepdir, "inputs/artifacts"), exist_ok=True)
        for name, art in artifacts.items():
            art_path = os.path.join(stepdir, "inputs/artifacts/%s" % name)
            if isinstance(art.source, str) and art.source.startswith("{{"):
                if "/" in art.source:
                    i = art.source.find("}}")
                    art.sub_path = art.source[i+3:]
                    art.source = art.source[:i+2]
                art.source = get_var(art.source, scope)
            if isinstance(art.source, S3Artifact) and not hasattr(
                    art.source, "local_path"):
                path = os.path.abspath("download/%s" % randstr())
                art.source.download(path=path, debug_download=True,
                                    remove_catalog=False)
                fs = os.listdir(path)
                if len(fs) == 1 and os.path.isfile(os.path.join(path,
                                                                fs[0])):
                    os.rename(os.path.join(path, fs[0]), path + ".bk")
                    os.rmdir(path)
                    os.rename(path + ".bk", path)
                art.source.local_path = path
            if isinstance(art.source, (InputArtifact, OutputArtifact,
                                       LocalArtifact, S3Artifact)):
                if art.sub_path is not None:
                    sub_path = art.sub_path
                    if item is not None:
                        sub_path = render_item(sub_path, item)
                    os.symlink(os.path.join(art.source.local_path, sub_path),
                               art_path)
                elif isinstance(
                        art.source,
                        InputArtifact) and art.optional and not hasattr(
                            art.source, 'local_path'):
                    pass
                else:
                    os.symlink(art.source.local_path, art_path)
            elif isinstance(art.source, str):
                with open(art_path, "w") as f:
                    f.write(art.source)
            elif not ignore_nonexist:
                raise RuntimeError("Not supported: ", art.source)

    def record_output_parameters(self, stepdir, parameters):
        os.makedirs(os.path.join(stepdir, "outputs/parameters"), exist_ok=True)
        for name, par in parameters.items():
            par_path = os.path.join(stepdir,
                                    "outputs/parameters/%s" % name)
            if isinstance(par.value, str):
                value = par.value
            else:
                value = jsonpickle.dumps(par.value)
            with open(par_path, "w") as f:
                f.write(value)
            if par.type is not None:
                os.makedirs(os.path.join(
                    stepdir, "outputs/parameters/.dflow"), exist_ok=True)
                with open(os.path.join(
                        stepdir, "outputs/parameters/.dflow/%s" % name),
                        "w") as f:
                    f.write(jsonpickle.dumps({"type": str(par.type)}))
            if par.global_name is not None:
                os.makedirs(os.path.join(stepdir, "../outputs/parameters"),
                            exist_ok=True)
                global_par_path = os.path.join(
                    stepdir, "../outputs/parameters/%s" % par.global_name)
                if os.path.exists(global_par_path):
                    os.remove(global_par_path)
                os.symlink(par_path, global_par_path)

    def record_output_artifacts(self, stepdir, artifacts):
        os.makedirs(os.path.join(stepdir, "outputs/artifacts"), exist_ok=True)
        for name, art in artifacts.items():
            art_path = os.path.join(stepdir, "outputs/artifacts/%s" % name)
            os.symlink(art.local_path, art_path)
            if art.global_name is not None:
                os.makedirs(os.path.join(stepdir, "../outputs/artifacts"),
                            exist_ok=True)
                global_art_path = os.path.join(
                    stepdir, "../outputs/artifacts/%s" % art.global_name)
                if os.path.exists(global_art_path):
                    os.remove(global_art_path)
                os.symlink(art_path, global_art_path)

    def load_output_parameters(self, stepdir, parameters):
        for name, par in parameters.items():
            par_path = os.path.join(stepdir,
                                    "outputs/parameters/%s" % name)
            with open(par_path, "r") as f:
                if par.type is None or par.type == str:
                    par.value = f.read()
                else:
                    par.value = jsonpickle.loads(f.read())

    def load_output_artifacts(self, stepdir, artifacts):
        for name, art in artifacts.items():
            art_path = os.path.join(stepdir,
                                    "outputs/artifacts/%s" % name)
            art.local_path = art_path

    def exec(self, scope, parameters, item=None):
        """
        directory structure:
        step-xxxxx
        |- inputs
           |- parameters
           |- artifacts
        |- outputs
           |- parameters
           |- artifacts
        |- script
        |- workdir
        """
        self.phase = "Running"

        # render item
        if item is not None:
            for name, par in parameters.items():
                if isinstance(par.value, str):
                    par.value = render_item(par.value, item)

        import os
        cwd = os.getcwd()
        if "dflow_key" in parameters:
            step_id = parameters["dflow_key"].value
            stepdir = os.path.abspath(step_id)
            if os.path.exists(stepdir):
                self.load_output_parameters(stepdir,
                                            self.outputs.parameters)
                self.load_output_artifacts(stepdir,
                                           self.outputs.artifacts)

                os.chdir(cwd)
                with open(os.path.join(stepdir, "phase"), "r") as f:
                    self.phase = f.read()
                return
            os.makedirs(stepdir)
        else:
            while True:
                step_id = self.name + "-" + randstr()
                stepdir = os.path.abspath(step_id)
                if not os.path.exists(stepdir):
                    os.makedirs(stepdir)
                    break

        self.stepdir = stepdir
        with open(os.path.join(stepdir, "type"), "w") as f:
            f.write("Pod")
        with open(os.path.join(stepdir, "phase"), "w") as f:
            f.write("Running")
        workdir = os.path.join(stepdir, "workdir")
        os.makedirs(workdir, exist_ok=True)
        os.chdir(workdir)

        self.record_input_parameters(stepdir, parameters)

        self.record_input_artifacts(stepdir, self.inputs.artifacts, item,
                                    scope)

        # prepare inputs artifacts
        for name, art in self.inputs.artifacts.items():
            art_path = os.path.join(stepdir, "inputs/artifacts/%s" % name)
            path = self.template.inputs.artifacts[name].path
            path = "%s/%s" % (workdir, path)
            path = render_script(path, parameters,
                                 scope.workflow_id, step_id)
            os.makedirs(os.path.dirname(
                os.path.abspath(path)), exist_ok=True)
            backup(path)
            if isinstance(
                    art.source,
                    InputArtifact) and art.source is None and art.optional:
                pass
            else:
                os.symlink(art_path, path)

        # clean output path
        for art in self.outputs.artifacts.values():
            path = art.path
            path = "%s/%s" % (workdir, path)
            backup(path)

        # render variables in the script
        script = self.template.script
        if self.executor is None:
            if hasattr(self.template, "tmp_root"):
                # do not modify self.template
                template = deepcopy(self.template)
                template.tmp_root = "%s%s" % (workdir, template.tmp_root)
                template.render_script()
                script = template.script
            else:
                script = script.replace("/tmp", "%s/tmp" % workdir)
        os.makedirs("%s/tmp" % workdir, exist_ok=True)
        script = render_script(script, parameters, scope.workflow_id, step_id)
        script_path = os.path.join(stepdir, "script")
        with open(script_path, "w") as f:
            f.write(script)

        import subprocess
        args = self.template.command + [script_path]
        with subprocess.Popen(
            args=args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ) as p:
            with open("%s/log.txt" % stepdir, "w") as f:
                line = p.stdout.readline().decode(sys.stdout.encoding)
                while line:
                    sys.stdout.write(line)
                    f.write(line)
                    line = p.stdout.readline().decode(sys.stdout.encoding)
            p.wait()
            ret_code = p.poll()
        if ret_code != 0:
            raise RuntimeError("Run %s failed" % args)

        # generate output parameters
        for name, par in self.outputs.parameters.items():
            path = par.value_from_path
            if path is not None:
                path = "%s/%s" % (workdir, path)
                with open(path, "r") as f:
                    if par.type is None or par.type == str:
                        par.value = f.read()
                    else:
                        par.value = jsonpickle.loads(f.read())
            elif hasattr(par, "value"):
                if isinstance(par.value, str):
                    par.value = render_script(
                        par.value, parameters, scope.workflow_id,
                        step_id)
        self.record_output_parameters(stepdir, self.outputs.parameters)

        # save artifacts
        for name, art in self.outputs.artifacts.items():
            path = art.path
            path = "%s/%s" % (workdir, path)
            art.local_path = path
            for save in self.template.outputs.artifacts[name].save:
                if isinstance(save, S3Artifact):
                    key = render_script(save.key, parameters,
                                        scope.workflow_id, step_id)
                    save_path = os.path.join(cwd, "..", key)
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)

                    def try_link(src, dst):
                        try:
                            os.symlink(src, dst)
                        except Exception:
                            pass
                    merge_dir(path, save_path, try_link)
                    art.local_path = save_path
        self.record_output_artifacts(stepdir, self.outputs.artifacts)

        os.chdir(cwd)
        self.phase = "Succeeded"
        with open(os.path.join(stepdir, "phase"), "w") as f:
            f.write("Succeeded")

    def exec_with_queue(self, scope, parameters, order, queue, item, conf,
                        s3_conf):
        try:
            config.update(conf)
            s3_config.update(s3_conf)
            self.exec(scope, parameters, item)
            queue.put((order, self))
        except Exception:
            import traceback
            traceback.print_exc()
            queue.put((order, None))


def render_item(expr, item):
    i = expr.find("{{")
    while i >= 0:
        j = expr.find("}}", i+2)
        var = expr[i+2:j]
        fields = var.split(".")
        if expr[i:i+3] == "{{=":
            value = eval(expr[i+3:j], {"item": item})
            expr = expr[:i] + value.strip() + expr[j+2:]
        elif fields[0] == "item":
            value = item
            for key in fields[1:]:
                value = value[key]
            value = value if isinstance(value, str) else \
                jsonpickle.dumps(value)
            expr = expr[:i] + value.strip() + expr[j+2:]
        i = expr.find("{{", i+1)
    return expr


def render_expr(expr, scope):
    # render variables
    i = expr.find("{{")
    while i >= 0:
        j = expr.find("}}", i+2)
        if expr[:3] == "{{=":
            value = Expression(replace_argo_func(expr[3:-2])).eval(scope)
            value = value if isinstance(value, str) else \
                jsonpickle.dumps(value)
            expr = expr[:i] + value.strip() + expr[j+2:]
        else:
            var = get_var(expr[i:j+2], scope)
            if var:
                value = var.value
                value = value if isinstance(value, str) else \
                    jsonpickle.dumps(value)
                expr = expr[:i] + value.strip() + expr[j+2:]
        i = expr.find("{{", i+1)
    return expr


def get_var(expr, scope):
    expr = str(expr)
    assert expr[:2] == "{{" and expr[-2:] == "}}", "Parse failed: %s" % expr
    fields = expr[2:-2].split(".")
    if fields[:2] == ["inputs", "parameters"]:
        name = fields[2]
        return scope.inputs.parameters[name]
    elif fields[:2] == ["inputs", "artifacts"]:
        name = fields[2]
        return scope.inputs.artifacts[name]
    elif fields[0] in ["steps", "tasks"] and \
            fields[2:4] == ["outputs", "parameters"]:
        step_name = fields[1]
        name = fields[4]
        for step in scope:
            if isinstance(step, list):
                for ps in step:
                    if ps.name == step_name:
                        return ps.outputs.parameters[name]
            elif step.name == step_name:
                return step.outputs.parameters[name]
        raise RuntimeError("Parse failed: %s" % expr)
    elif fields[0] in ["steps", "tasks"] and \
            fields[2:4] == ["outputs", "artifacts"]:
        step_name = fields[1]
        name = fields[4]
        for step in scope:
            if isinstance(step, list):
                for ps in step:
                    if ps.name == step_name:
                        return ps.outputs.artifacts[name]
            elif step.name == step_name:
                return step.outputs.artifacts[name]
        raise RuntimeError("Parse failed: %s" % expr)
    elif fields[0] == "item":
        return None  # ignore
    else:
        raise RuntimeError("Not supported: %s" % expr)


def eval_expr(expr):
    if expr == "true":
        return True
    elif expr == "false":
        return False
    expr_list = expr.split()
    assert (len(expr_list) == 3), "Expression (%s) not supported" % expr
    operator = expr_list[1]
    if operator == "==":
        return expr_list[0] == expr_list[2]
    elif operator == "!=":
        return expr_list[0] != expr_list[2]
    expr_right = float(expr_list[2])
    expr_left = float(expr_list[0])
    if operator == "<=":
        return expr_left <= expr_right
    elif operator == "<":
        return expr_left < expr_right
    elif operator == ">=":
        return expr_left >= expr_right
    elif operator == ">":
        return expr_left > expr_right
    raise RuntimeError("Evaluate expression failed: %s" % expr)


def render_script(script, parameters, workflow_id=None, step_id=None):
    if workflow_id is not None:
        script = script.replace("{{workflow.name}}", workflow_id)
    if step_id is not None:
        script = script.replace("{{pod.name}}", step_id)
    i = script.find("{{")
    while i >= 0:
        j = script.find("}}", i+2)
        var = script[i+2:j]
        if var[:1] == "=":
            var = var[1:]
        fields = var.split(".")
        if fields[0] == "inputs" and fields[1] == "parameters":
            par = fields[2]
            if par in parameters:
                value = parameters[par].value
                script = script[:i] + (value if isinstance(value, str) else
                                       jsonpickle.dumps(value)) + script[j+2:]
        else:
            raise RuntimeError("Not supported: %s" % var)
        i = script.find("{{", i+1)
    return script


def backup(path):
    import os
    import shutil
    cnt = 0
    bk = path
    while os.path.exists(bk) or os.path.islink(bk):
        cnt += 1
        bk = path + ".bk%s" % cnt
    if bk != path:
        shutil.move(path, bk)


def replace_argo_func(expr):
    expr = expr.replace("toJson", "str")
    expr = expr.replace("sprig.fromJson", "eval")
    expr = expr.replace("sprig.untilStep",
                        "(lambda *x: list(range(*list(map(int, x)))))")
    expr = expr.replace("sprig.atoi", "int")
    expr = expr.replace(" ? ", " and ")
    expr = expr.replace(" : ", " or ")
    expr = expr.replace("asFloat", "float")
    return expr
