from copy import deepcopy
import jsonpickle
from argo.workflows.client import (
    V1alpha1WorkflowStep,
    V1alpha1Arguments,
    V1VolumeMount,
    V1alpha1ContinueOn
)
from .io import InputParameter, OutputParameter, PVC, ArgoVar
from .op_template import ShellOPTemplate, PythonScriptOPTemplate
from .utils import copy_file
from .util_ops import CheckNumSuccess, CheckSuccessRatio
from .client import V1alpha1Sequence

def argo_range(*args):
    """
    Return a str representing a range of integer in Argo
    It receives 1-3 arguments, which is similar to the function `range` in Python
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

def argo_sequence(count=None, start=None, end=None, format=None):
    """
    Return a numeric sequence in Argo
    :param count: length of the sequence, can be an Argo parameter
    :param start: start number of the sequence, can be an Argo parameter
    :param end: end number of the sequence, can be an Argo parameter
    :param format: output the sequence with format
    :return:
    """
    if isinstance(count, ArgoVar):
        count = "{{=%s}}" % count.expr
    if isinstance(start, ArgoVar):
        start = "{{=%s}}" % start.expr
    if isinstance(end, ArgoVar):
        end = "{{=%s}}" % end.expr
    return V1alpha1Sequence(count=count, start=start, end=end, format=format)

def argo_len(param):
    """
    Return the length of a list which is an Argo parameter
    :param param: the Argo parameter which is a list
    :return:
    """
    return ArgoVar("len(sprig.fromJson(%s))" % param.expr)

def if_expression(_if, _then, _else):
    """
    Return an if expression in Argo
    :param _if: a bool expression, which may be a comparison of two Argo parameters
    :param _then: value returned if the condition is satisfied
    :param _else: value returned if the condition is not satisfied
    :return:
    """
    if isinstance(_if, ArgoVar):
        _if = _if.expr
    if isinstance(_then, ArgoVar):
        _then = _then.expr
    if isinstance(_else, ArgoVar):
        _else = _else.expr
    return "%s ? %s : %s" % (_if, _then, _else)

class Step:
    def __init__(self, name, template, parameters=None, artifacts=None, when=None, with_param=None, continue_on_failed=False,
            continue_on_num_success=None, continue_on_success_ratio=None, with_sequence=None, key=None, executor=None):
        """
        Instantiate a step
        :param name: the name of the step
        :param template: OP template the step uses
        :param parameters: input parameters passed to the step as arguments
        :param artifacts: input artifacts passed to the step as arguments
        :param when: conditional step if the condition is satisfied
        :param with_param: generate parallel steps with respect to a list as a parameter
        :param continue_on_failed: continue if the step fails
        :param continue_on_num_success: continue if the success number of the generated parallel steps greater than certain number
        :param continue_on_success_ratio: continue if the success ratio of the generated parallel steps greater than certain number
        :param with_sequence: generate parallel steps with respect to a sequence
        :param key: the key of the step
        :param executor: define the executor to execute the script
        :return:
        """
        self.name = name
        self.id = self.name
        self.template = template
        self.inputs = deepcopy(self.template.inputs)
        self.outputs = deepcopy(self.template.outputs)
        self.inputs.set_step_id(self.id)
        self.outputs.set_step_id(self.id)
        self.continue_on_failed = continue_on_failed
        self.continue_on_num_success = continue_on_num_success
        self.continue_on_success_ratio = continue_on_success_ratio
        self.check_step = None

        if parameters is not None:
            self.set_parameters(parameters)

        if artifacts is not None:
            self.set_artifacts(artifacts)

        self.when = when
        self.with_param = with_param
        self.with_sequence = with_sequence
        self.key = key
        self.executor = executor

    def __repr__(self):
        return self.id
    
    def set_parameters(self, parameters):
        for k, v in parameters.items():
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

    def convert_to_argo(self):
        if self.key is not None:
            self.template.key = self.key
            self.inputs.parameters["dflow_key"] = InputParameter(value=str(self.key))
            if hasattr(self.template, "slices") and self.template.slices is not None and self.template.slices.output_artifact is not None:
                self.inputs.parameters["dflow_group_key"] = InputParameter(value=str(self.key).replace("{{item}}", "group"))
        argo_parameters = []
        for par in self.inputs.parameters.values():
            argo_parameters.append(par.convert_to_argo())

        new_template = None

        argo_artifacts = []
        pvc_arts = []
        for art in self.inputs.artifacts.values():
            if isinstance(art.source, PVC):
                pvc_arts.append((art.source, art))
            elif art.source is None and art.optional == True:
                pass
            else:
                argo_artifacts.append(art.convert_to_argo())

        if len(pvc_arts) > 0:
            if new_template is None:
                new_template = deepcopy(self.template)
                new_template.name = self.template.name + "-" + self.name
            if (isinstance(new_template, ShellOPTemplate)):
                for pvc, art in pvc_arts:
                    del new_template.inputs.artifacts[art.name]
                    new_template.script = "ln -s /tmp/mnt/%s %s\n" % (pvc.subpath, art.path) + new_template.script
                    new_template.mounts.append(V1VolumeMount(name=pvc.name, mount_path="/tmp/mnt"))
                    new_template.pvcs.append(pvc)
            elif (isinstance(new_template, PythonScriptOPTemplate)):
                for pvc, art in pvc_arts:
                    del new_template.inputs.artifacts[art.name]
                    new_template.script = "os.system('ln -s /tmp/mnt/%s %s')\n" % (pvc.subpath, art.path) + new_template.script
                    new_template.mounts.append(V1VolumeMount(name=pvc.name, mount_path="/tmp/mnt"))
                    new_template.pvcs.append(pvc)
                new_template.script = "import os\n" + new_template.script
            else:
                raise RuntimeError("Unsupported type of OPTemplate to mount PVC")
        
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
                    new_template.mounts.append(V1VolumeMount(name=pvc.name, mount_path="/tmp/mnt"))
                    new_template.script += "cp -r %s /tmp/mnt/%s\n" % (art.path, pvc.subpath)
                    new_template.pvcs.append(pvc)
            elif (isinstance(new_template, PythonScriptOPTemplate)):
                new_template.script += "\nimport os\n"
                for pvc, art in pvc_arts:
                    new_template.mounts.append(V1VolumeMount(name=pvc.name, mount_path="/tmp/mnt"))
                    new_template.script += "os.system('cp -r %s /tmp/mnt/%s')\n" % (art.path, pvc.subpath)
                    new_template.pvcs.append(pvc)
            else:
                raise RuntimeError("Unsupported type of OPTemplate to mount PVC")

        if self.continue_on_num_success or self.continue_on_success_ratio is not None:
            self.continue_on_failed = True
            if new_template is None:
                new_template = deepcopy(self.template)
                new_template.name = self.template.name + "-" + self.name
            if (isinstance(new_template, ShellOPTemplate)):
                new_template.outputs.parameters["dflow_success_tag"] = OutputParameter(value_from_path="/tmp/success_tag", default="0")
                new_template.script += "\n"
                new_template.script += "echo 1 > /tmp/success_tag\n"
            elif (isinstance(new_template, PythonScriptOPTemplate)):
                new_template.outputs.parameters["dflow_success_tag"] = OutputParameter(value_from_path="/tmp/success_tag", default="0")
                new_template.script += "\n"
                new_template.script += "open('/tmp/success_tag', 'w').write('1')\n"
            else:
                raise RuntimeError("Unsupported type of OPTemplate for continue_on_num_success or continue_on_success_ratio")

        if new_template is not None:
            self.template = new_template
            self.inputs = deepcopy(self.template.inputs)
            self.outputs = deepcopy(self.template.outputs)
            self.inputs.set_step_id(self.id)
            self.outputs.set_step_id(self.id)

        if self.continue_on_num_success is not None:
            self.check_step = Step(
                name="%s-check-num-success" % self.name, template=CheckNumSuccess(image=self.template.image),
                parameters={
                    "success": self.outputs.parameters["dflow_success_tag"],
                    "threshold": self.continue_on_num_success
                }
            )
        elif self.continue_on_success_ratio is not None:
            self.check_step = Step(
                name="%s-check-success-ratio" % self.name, template=CheckSuccessRatio(image=self.template.image),
                parameters={
                    "success": self.outputs.parameters["dflow_success_tag"],
                    "threshold": self.continue_on_success_ratio
                }
            )

        if isinstance(self.with_param, ArgoVar):
            self.with_param = "{{=%s}}" % self.with_param.expr

        if self.executor is not None:
            new_template = ShellOPTemplate(name=self.template.name + "-remote", inputs=self.template.inputs,
                outputs=self.template.outputs, image=self.executor.image, command=self.executor.command, script=None, volumes=self.template.volumes, mounts=self.template.mounts,
                init_progress=self.template.init_progress, timeout=self.template.timeout, retry_strategy=self.template.retry_strategy,
                memoize_key=self.template.memoize_key, key=self.template.key)
            new_template.script = self.executor.get_script(self.template.command, self.template.script)
            self.template = new_template

        return V1alpha1WorkflowStep(
            name=self.name, template=self.template.name, arguments=V1alpha1Arguments(
                parameters=argo_parameters,
                artifacts=argo_artifacts
            ), when=self.when, with_param=self.with_param, with_sequence=self.with_sequence,
            continue_on=V1alpha1ContinueOn(failed=self.continue_on_failed)
        )

    def run(self, context):
        import os
        import shutil
        import random, string
        from .io import InputParameter, OutputParameter, InputArtifact, OutputArtifact
        from .steps import Steps
        from copy import copy

        expr = self.when
        if expr is not None:
            # render variables
            i = expr.find("{{")
            while i >= 0:
                j = expr.find("}}", i+2)
                var = expr[i+2:j]
                fields = var.split(".")
                if fields[0] == "inputs" and fields[1] == "parameters":
                    name = fields[2]
                    value = context.inputs.parameters[name].value
                elif fields[0] == "steps" and fields[2] == "outputs" and fields[3] == "parameters":
                    step_name = fields[1]
                    name = fields[4]
                    value = None
                    for step in context.steps:
                        if step.name == step_name:
                            value = step.outputs.parameters[name].value
                            break
                    if value is None:
                        raise RuntimeError("Parse failed: ", var)
                else:
                    raise RuntimeError("Not supported: ", var)

                value = value if isinstance(value, str) else jsonpickle.dumps(value)
                expr = expr[:i] + value.strip() + expr[j+2:]
                i = expr.find("{{")

            if not eval_bool_expr(expr):
                return

        if isinstance(self.template, Steps):
            steps = copy(self.template) # shallow copy to avoid changing each step
            steps.inputs = deepcopy(self.template.inputs)

            # override default inputs with arguments
            for name, par in self.inputs.parameters.items():
                if isinstance(par.value, (InputParameter, OutputParameter)):
                    steps.inputs.parameters[name].value = par.value.value
                else:
                    steps.inputs.parameters[name].value = par.value

            for name, art in self.inputs.artifacts.items():
                steps.inputs.artifacts[name].source = art.source

            steps.run()
            return

        workdir = self.name + "-" + "".join(random.sample(string.digits + string.ascii_lowercase, 5))
        os.makedirs(workdir, exist_ok=True)
        os.chdir(workdir)

        # render artifacts
        os.makedirs("inputs/artifacts", exist_ok=True)
        for art in self.inputs.artifacts.values():
            if isinstance(art.source, (InputArtifact, OutputArtifact)):
                os.symlink(art.source.local_path, "inputs/artifacts/%s" % art.name)
            elif isinstance(art.source, str):
                with open("inputs/artifacts/%s" % art.name, "w") as f:
                    f.write(art.source)
            else:
                raise RuntimeError("Not supported: ", art.source)
            os.makedirs(os.path.dirname(os.path.abspath(art.path)), exist_ok=True)
            backup(art.path)
            os.symlink(os.path.abspath("inputs/artifacts/%s" % art.name), art.path)

        # clean output path
        for art in self.outputs.artifacts.values():
            backup(art.path)

        # render parameters
        os.makedirs("inputs/parameters", exist_ok=True)
        parameters = deepcopy(self.inputs.parameters)
        for par in parameters.values():
            value = par.value
            if isinstance(value, (InputParameter, OutputParameter)):
                if value.step_id is None: # obtain steps parameters from context
                    par.value = context.inputs.parameters[value.name].value
                else:
                    par.value = value.value
            with open("inputs/parameters/%s" % par.name, "w") as f:
                f.write(par.value if isinstance(par.value, str) else jsonpickle.dumps(par.value))

        script = self.template.script
        # render variables in the script
        i = script.find("{{")
        while i >= 0:
            j = script.find("}}", i+2)
            var = script[i+2:j]
            fields = var.split(".")
            if fields[0] == "inputs" and fields[1] == "parameters":
                par = fields[2]
                value = parameters[par].value
                script = script[:i] + (value if isinstance(value, str) else jsonpickle.dumps(value)) + script[j+2:]
            else:
                raise RuntimeError("Not supported: ", var)
            i = script.find("{{")
        with open("script", "w") as f:
            f.write(script)

        os.system(" ".join(self.template.command) + " script")

        # save parameters
        os.makedirs("outputs/parameters", exist_ok=True)
        for par in self.outputs.parameters.values():
            with open(par.value_from_path, "r") as f:
                par.value = f.read()
            with open("outputs/parameters/%s" % par.name, "w") as f:
                f.write(par.value)

        # save artifacts
        os.makedirs("outputs/artifacts", exist_ok=True)
        for art in self.outputs.artifacts.values():
            copy_file(art.path, "outputs/artifacts/%s" % art.name)
            art.local_path = os.path.abspath("outputs/artifacts/%s" % art.name)

        os.chdir("..")

def eval_bool_expr(expr):
    # For the original evaluator in argo, please refer to https://github.com/antonmedv/expr
    import os
    expr = expr.replace("<=", "-le")
    expr = expr.replace(">=", "-ge")
    expr = expr.replace("<", "-lt")
    expr = expr.replace(">", "-gt")
    result = os.popen("sh -c 'if [[ %s ]]; then echo 1; else echo 0; fi'" % expr).read().strip()
    if result == "1":
        return True
    elif result == "0":
        return False
    else:
        raise RuntimeError("Evaluate expression failed: ", expr)

def backup(path):
    import os, shutil
    cnt = 0
    bk = path
    while os.path.exists(bk) or os.path.islink(bk):
        cnt += 1
        bk = path + ".bk%s" % cnt
    if bk != path:
        shutil.move(path, bk)
