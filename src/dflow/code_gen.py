import inspect
import json

import jsonpickle

from .common import (input_artifact_pattern, input_parameter_pattern,
                     step_output_artifact_pattern,
                     step_output_parameter_pattern,
                     task_output_artifact_pattern,
                     task_output_parameter_pattern)
from .config import config
from .dag import DAG
from .io import InputArtifact, InputParameter, OutputArtifact, OutputParameter
from .op_template import ScriptOPTemplate
from .python import PythonOPTemplate
from .step import Step
from .steps import Steps
from .utils import Variable, evalable_repr
from .workflow import Workflow


class CodeGenerator:
    def __init__(self, graph):
        self.graph = graph
        self.imports = set()
        self.code = ""

    def get_var_name(self, name):
        return name.replace("-", "_")

    def get_kwargs(self, template, cls):
        kwargs = {}
        sign = inspect.signature(cls.__init__).parameters
        for k, v in template.items():
            if k in sign:
                if v == sign[k].default:
                    continue
                if sign[k].default is None and v in [[], {}]:
                    continue
            kwargs[k] = v
        return kwargs

    def render_python_op_template(self, var_name, template):
        assert template.pop("type") == "PythonOPTemplate"
        op = template.pop("op")
        if op["module"] in ["__main__", "__mp_main__"]:
            self.code += op["source"]
        else:
            self.imports.add((op["module"], op["name"]))

        kwargs = self.get_kwargs(template, PythonOPTemplate)
        if "command" in kwargs and kwargs["command"] == ["python3"]:
            del kwargs["command"]
        kwargs["op_class"] = Variable(op["name"])
        self.imports.add(("dflow.python", "PythonOPTemplate"))
        self.code += "%s = PythonOPTemplate(%s)\n" % (var_name, ", ".join([
            "%s=%s" % (k, evalable_repr(v, self.imports))
            for k, v in kwargs.items()]))

    def render_script_op_template(self, var_name, template):
        assert template.pop("type") == "ScriptOPTemplate"
        kwargs = self.get_kwargs(template, ScriptOPTemplate)
        self.imports.add(("dflow", "ScriptOPTemplate"))
        self.code += "%s = ScriptOPTemplate(%s)\n" % (var_name, ", ".join([
            "%s=%s" % (k, evalable_repr(v, self.imports))
            for k, v in kwargs.items()]))

    def render_steps(self, var_name, template):
        assert template.pop("type") == "Steps"
        inputs = template.pop("inputs", {})
        outputs = template.pop("outputs", {})
        steps = template.pop("steps", {})
        kwargs = self.get_kwargs(template, Steps)
        self.imports.add(("dflow", "Steps"))
        self.code += "%s = Steps(%s)\n" % (var_name, ", ".join([
            "%s=%s" % (k, evalable_repr(v, self.imports))
            for k, v in kwargs.items()]))

        for name, par in inputs.get("parameters", {}).items():
            kwargs = self.get_kwargs(par, InputParameter)
            kwargs.pop("name", None)
            self.imports.add(("dflow", "InputParameter"))
            self.code += "%s.inputs.parameters['%s'] = InputParameter(%s)\n" \
                % (var_name, name, ", ".join(["%s=%s" % (k, evalable_repr(
                    v, self.imports)) for k, v in kwargs.items()]))

        for name, art in inputs.get("artifacts", {}).items():
            kwargs = self.get_kwargs(art, InputArtifact)
            kwargs.pop("name", None)
            self.imports.add(("dflow", "InputArtifact"))
            self.code += "%s.inputs.artifacts['%s'] = InputArtifact(%s)\n" % \
                (var_name, name, ", ".join(["%s=%s" % (k, evalable_repr(
                    v, self.imports)) for k, v in kwargs.items()]))

        step_dict = {}
        for step in steps:
            parallel_steps = []
            for ps in step:
                kwargs = self.get_kwargs(ps, Step)
                if "util_image" in kwargs and kwargs["util_image"] == config[
                        "util_image"]:
                    del kwargs["util_image"]
                if kwargs["template"] in self.templates:
                    kwargs["template"] = Variable(
                        self.templates[kwargs["template"]])
                elif kwargs["template"] == template["name"]:
                    kwargs["template"] = Variable(var_name)
                for name, par in kwargs.get("parameters", {}).items():
                    if isinstance(par, str):
                        match = input_parameter_pattern.match(par)
                        if match:
                            kwargs["parameters"][name] = Variable(
                                "%s.inputs.parameters['%s']" % (
                                    var_name, match.group(1)))
                        match = step_output_parameter_pattern.match(par)
                        if match:
                            kwargs["parameters"][name] = Variable(
                                "%s.outputs.parameters['%s']" % (
                                    step_dict[match.group(1)], match.group(2)))
                    elif isinstance(par, dict) and "py/object" in par:
                        self.imports.add((None, "jsonpickle"))
                        kwargs["parameters"][name] = Variable(
                            "jsonpickle.loads(%s)" % repr(json.dumps(par)))
                for name, art in kwargs.get("artifacts", {}).items():
                    if isinstance(art, str):
                        match = input_artifact_pattern.match(art)
                        if match:
                            kwargs["artifacts"][name] = Variable(
                                "%s.inputs.artifacts['%s']" % (
                                    var_name, match.group(1)))
                        match = step_output_artifact_pattern.match(art)
                        if match:
                            kwargs["artifacts"][name] = Variable(
                                "%s.outputs.artifacts['%s']" % (
                                    step_dict[match.group(1)], match.group(2)))

                self.imports.add(("dflow", "Step"))
                step_var_name = "step_" + self.get_var_name(ps["name"])
                self.code += "%s = Step(%s)\n" % (
                    step_var_name, ", ".join(["%s=%s" % (k, evalable_repr(
                        v, self.imports)) for k, v in kwargs.items()]))
                step_dict[ps["name"]] = step_var_name
                parallel_steps.append(step_var_name)

            if len(parallel_steps) == 1:
                self.code += "%s.add(%s)\n" % (var_name, parallel_steps[0])
            else:
                self.code += "%s.add([%s])\n" % (
                    var_name, ", ".join(parallel_steps))

        for name, par in outputs.get("parameters", {}).items():
            kwargs = self.get_kwargs(par, OutputParameter)
            kwargs.pop("name", None)
            _from = kwargs.get("value_from_parameter")
            if isinstance(_from, str):
                match = input_parameter_pattern.match(_from)
                if match:
                    kwargs["value_from_parameter"] = Variable(
                        "%s.inputs.parameters['%s']" % (
                            var_name, match.group(1)))
                match = step_output_parameter_pattern.match(_from)
                if match:
                    kwargs["value_from_parameter"] = Variable(
                        "%s.outputs.parameters['%s']" % (
                            step_dict[match.group(1)], match.group(2)))
            self.imports.add(("dflow", "OutputParameter"))
            self.code += "%s.outputs.parameters['%s'] = OutputParameter(%s)\n"\
                % (var_name, name, ", ".join(["%s=%s" % (k, evalable_repr(
                    v, self.imports)) for k, v in kwargs.items()]))

        for name, art in outputs.get("artifacts", {}).items():
            kwargs = self.get_kwargs(art, OutputArtifact)
            kwargs.pop("name", None)
            if "archive" in kwargs and kwargs["archive"] == config[
                    "archive_mode"]:
                del kwargs["archive"]
            _from = kwargs.get("_from")
            if isinstance(_from, str):
                match = input_artifact_pattern.match(_from)
                if match:
                    kwargs["_from"] = Variable(
                        "%s.inputs.artifacts['%s']" % (
                            var_name, match.group(1)))
                match = step_output_artifact_pattern.match(_from)
                if match:
                    kwargs["_from"] = Variable(
                        "%s.outputs.artifacts['%s']" % (
                            step_dict[match.group(1)], match.group(2)))
            self.imports.add(("dflow", "OutputArtifact"))
            self.code += "%s.outputs.artifacts['%s'] = OutputArtifact(%s)\n" %\
                (var_name, name, ", ".join(["%s=%s" % (k, evalable_repr(
                    v, self.imports)) for k, v in kwargs.items()]))

    def render_dag(self, var_name, template):
        assert template.pop("type") == "DAG"
        inputs = template.pop("inputs", {})
        outputs = template.pop("outputs", {})
        tasks = template.pop("tasks", {})
        kwargs = self.get_kwargs(template, DAG)
        self.imports.add(("dflow", "DAG"))
        self.code += "%s = DAG(%s)\n" % (var_name, ", ".join([
            "%s=%s" % (k, evalable_repr(v, self.imports))
            for k, v in kwargs.items()]))

        for name, par in inputs.get("parameters", {}).items():
            kwargs = self.get_kwargs(par, InputParameter)
            kwargs.pop("name", None)
            self.imports.add(("dflow", "InputParameter"))
            self.code += "%s.inputs.parameters['%s'] = InputParameter(%s)\n" \
                % (var_name, name, ", ".join(["%s=%s" % (k, evalable_repr(
                    v, self.imports)) for k, v in kwargs.items()]))

        for name, art in inputs.get("artifacts", {}).items():
            kwargs = self.get_kwargs(art, InputArtifact)
            kwargs.pop("name", None)
            self.imports.add(("dflow", "InputArtifact"))
            self.code += "%s.inputs.artifacts['%s'] = InputArtifact(%s)\n" % \
                (var_name, name, ", ".join(["%s=%s" % (k, evalable_repr(
                    v, self.imports)) for k, v in kwargs.items()]))

        task_dict = {}
        for task in tasks:
            task_var_name = "task_" + self.get_var_name(task["name"])
            task_dict[task["name"]] = task_var_name

        kwargs_dict = {}
        dependencies_dict = {t: [] for t in task_dict}
        for task in tasks:
            dependencies = task.get("dependencies", [])
            kwargs = self.get_kwargs(task, Step)
            if dependencies:
                dependencies_dict[task["name"]].extend(dependencies)
                kwargs["dependencies"] = [Variable(task_dict[t])
                                          for t in dependencies]
            if "util_image" in kwargs and kwargs["util_image"] == config[
                    "util_image"]:
                del kwargs["util_image"]
            if kwargs["template"] in self.templates:
                kwargs["template"] = Variable(
                    self.templates[kwargs["template"]])
            elif kwargs["template"] == template["name"]:
                kwargs["template"] = Variable(var_name)
            for name, par in kwargs.get("parameters", {}).items():
                if isinstance(par, str):
                    match = input_parameter_pattern.match(par)
                    if match:
                        kwargs["parameters"][name] = Variable(
                            "%s.inputs.parameters['%s']" % (
                                var_name, match.group(1)))
                    match = task_output_parameter_pattern.match(par)
                    if match:
                        kwargs["parameters"][name] = Variable(
                            "%s.outputs.parameters['%s']" % (
                                task_dict[match.group(1)], match.group(2)))
                        dependencies_dict[task["name"]].append(match.group(1))
                elif isinstance(par, dict) and "py/object" in par:
                    self.imports.add((None, "jsonpickle"))
                    kwargs["parameters"][name] = Variable(
                        "jsonpickle.loads(%s)" % repr(json.dumps(par)))
            for name, art in kwargs.get("artifacts", {}).items():
                if isinstance(art, str):
                    match = input_artifact_pattern.match(art)
                    if match:
                        kwargs["artifacts"][name] = Variable(
                            "%s.inputs.artifacts['%s']" % (
                                var_name, match.group(1)))
                    match = task_output_artifact_pattern.match(art)
                    if match:
                        kwargs["artifacts"][name] = Variable(
                            "%s.outputs.artifacts['%s']" % (
                                task_dict[match.group(1)], match.group(2)))
                        dependencies_dict[task["name"]].append(match.group(1))
            kwargs_dict[task["name"]] = kwargs

        while len(kwargs_dict) > 0:
            update = False
            for name, kwargs in list(kwargs_dict.items()):
                if all([t not in kwargs_dict
                        for t in dependencies_dict[name]]):
                    self.imports.add(("dflow", "Task"))
                    task_var_name = "task_" + self.get_var_name(name)
                    self.code += "%s = Task(%s)\n" % (
                        task_var_name, ", ".join(["%s=%s" % (k, evalable_repr(
                            v, self.imports)) for k, v in kwargs.items()]))
                    self.code += "%s.add(%s)\n" % (var_name, task_var_name)
                    update = True
                    del kwargs_dict[name]
            assert update, "Failed to resolve tasks: %s" % list(kwargs_dict)

        for name, par in outputs.get("parameters", {}).items():
            kwargs = self.get_kwargs(par, OutputParameter)
            kwargs.pop("name", None)
            _from = kwargs.get("value_from_parameter")
            if isinstance(_from, str):
                match = input_parameter_pattern.match(_from)
                if match:
                    kwargs["value_from_parameter"] = Variable(
                        "%s.inputs.parameters['%s']" % (
                            var_name, match.group(1)))
                match = task_output_parameter_pattern.match(_from)
                if match:
                    kwargs["value_from_parameter"] = Variable(
                        "%s.outputs.parameters['%s']" % (
                            task_dict[match.group(1)], match.group(2)))
            self.imports.add(("dflow", "OutputParameter"))
            self.code += "%s.outputs.parameters['%s'] = OutputParameter(%s)\n"\
                % (var_name, name, ", ".join(["%s=%s" % (k, evalable_repr(
                    v, self.imports)) for k, v in kwargs.items()]))

        for name, art in outputs.get("artifacts", {}).items():
            kwargs = self.get_kwargs(art, OutputArtifact)
            kwargs.pop("name", None)
            if "archive" in kwargs and kwargs["archive"] == config[
                    "archive_mode"]:
                del kwargs["archive"]
            _from = kwargs.get("_from")
            if isinstance(_from, str):
                match = input_artifact_pattern.match(_from)
                if match:
                    kwargs["_from"] = Variable(
                        "%s.inputs.artifacts['%s']" % (
                            var_name, match.group(1)))
                match = task_output_artifact_pattern.match(_from)
                if match:
                    kwargs["_from"] = Variable(
                        "%s.outputs.artifacts['%s']" % (
                            task_dict[match.group(1)], match.group(2)))
            self.imports.add(("dflow", "OutputArtifact"))
            self.code += "%s.outputs.artifacts['%s'] = OutputArtifact(%s)\n" %\
                (var_name, name, ", ".join(["%s=%s" % (k, evalable_repr(
                    v, self.imports)) for k, v in kwargs.items()]))

    def generate(self):
        self.graph = jsonpickle.loads(json.dumps(self.graph))
        entrypoint_type = self.graph["templates"][self.graph["entrypoint"]][
            "type"]

        self.templates = {}
        for name, template in list(self.graph["templates"].items()):
            if template["type"] == "PythonOPTemplate":
                var_name = self.get_var_name(name)
                self.render_python_op_template(var_name, template)
                self.templates[name] = var_name
                del self.graph["templates"][name]
            elif template["type"] == "ScriptOPTemplate":
                var_name = self.get_var_name(name)
                self.render_script_op_template(var_name, template)
                self.templates[name] = var_name
                del self.graph["templates"][name]

        while len(self.graph["templates"]) > 0:
            update = False
            for name, template in list(self.graph["templates"].items()):
                if template["type"] == "Steps":
                    if all([all([ps["template"] in self.templates or ps[
                        "template"] == name for ps in s])
                            for s in template["steps"]]):
                        var_name = self.get_var_name(name)
                        self.render_steps(var_name, template)
                        self.templates[name] = var_name
                        del self.graph["templates"][name]
                        update = True
                elif template["type"] == "DAG":
                    if all([t["template"] in self.templates or t[
                            "template"] == name for t in template["tasks"]]):
                        var_name = self.get_var_name(name)
                        self.render_dag(var_name, template)
                        self.templates[name] = var_name
                        del self.graph["templates"][name]
                        update = True
            assert update, "Failed to resolve templates: %s" % list(
                self.graph["templates"])

        del self.graph["templates"]
        entrypoint = self.templates[self.graph.pop("entrypoint")]
        kwargs = self.get_kwargs(self.graph, Workflow)
        if "namespace" in kwargs and kwargs["namespace"] == config[
                "namespace"]:
            del kwargs["namespace"]
        if entrypoint_type == "Steps":
            kwargs["steps"] = Variable(entrypoint)
        elif entrypoint_type == "DAG":
            kwargs["dag"] = Variable(entrypoint)
        self.imports.add(("dflow", "Workflow"))
        self.code += "wf = Workflow(%s)\n" % ", ".join(["%s=%s" % (
            k, evalable_repr(v, self.imports)) for k, v in kwargs.items()])

        module_imports = {}
        for m, i in self.imports:
            if m not in module_imports:
                module_imports[m] = []
            module_imports[m].append(i)
        import_code = ""
        for i in sorted(module_imports.pop(None, [])):
            import_code += "import %s\n" % i
        for m in sorted(module_imports):
            import_code += "from %s import %s\n" % (
                m, ", ".join(sorted(module_imports[m])))
        self.code = import_code + "\n" + self.code

        self.code += "wf.submit()\n"
        return self.code


def gen_code(graph):
    gen = CodeGenerator(graph)
    return gen.generate()
