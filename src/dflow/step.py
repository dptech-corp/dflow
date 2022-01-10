from copy import deepcopy
import jsonpickle
from argo.workflows.client import (
    V1alpha1WorkflowStep,
    V1alpha1Arguments,
    V1VolumeMount,
    V1alpha1ContinueOn
)
from .io import InputParameter, OutputParameter, PVC
from .op_template import ShellOPTemplate, PythonScriptOPTemplate
from .python.utils import copy_file
from .util_ops import CheckNumSuccess, CheckSuccessRatio

def argo_range(*args):
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
        start = "sprig.atoi(%s)" % start
    if isinstance(step, (InputParameter, OutputParameter)):
        step = "sprig.atoi(%s)" % step
    if isinstance(end, (InputParameter, OutputParameter)):
        end = "sprig.atoi(%s)" % end
    return "{{=toJson(sprig.untilStep(%s, %s, %s))}}" % (start, end, step)

class Step:
    def __init__(self, name, template, parameters=None, artifacts=None, when=None, with_param=None, continue_on_failed=False,
            continue_on_num_success=None, continue_on_success_ratio=None):
        self.name = name
        self.id = "steps.%s" % self.name
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

    def __repr__(self):
        return self.id
    
    def set_parameters(self, parameters):
        for k, v in parameters.items():
            self.inputs.parameters[k].value = v
    
    def set_artifacts(self, artifacts):
        for k, v in artifacts.items():
            self.inputs.artifacts[k].source = v

    def convert_to_argo(self):
        argo_parameters = []
        for par in self.inputs.parameters.values():
            argo_parameters.append(par.convert_to_argo())

        new_template = None

        argo_artifacts = []
        pvc_arts = []
        for art in self.inputs.artifacts.values():
            if isinstance(art.source, PVC):
                pvc_arts.append((art.source, art))
            else:
                argo_artifacts.append(art.convert_to_argo())

        if len(pvc_arts) > 0:
            if new_template is None:
                new_template = deepcopy(self.template)
                new_template.name = self.template.name + "-" + self.name
            if (isinstance(new_template, ShellOPTemplate)):
                for pvc, art in pvc_arts:
                    del new_template.inputs.artifacts[art.name]
                    new_template.script = "ln -s /tmp/mnt/%s/%s %s\n" % (pvc.relpath, pvc.name, art.path) + new_template.script
                    new_template.mounts.append(V1VolumeMount(name=pvc.pvcname, mount_path="/tmp/mnt"))
            elif (isinstance(new_template, PythonScriptOPTemplate)):
                for pvc, art in pvc_arts:
                    del new_template.inputs.artifacts[art.name]
                    new_template.script = "os.system('ln -s /tmp/mnt/%s/%s %s')\n" % (pvc.relpath, pvc.name, art.path) + new_template.script
                    new_template.mounts.append(V1VolumeMount(name=pvc.pvcname, mount_path="/tmp/mnt"))
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
                    new_template.mounts.append(V1VolumeMount(name=pvc.pvcname, mount_path="/tmp/mnt"))
                    new_template.script += "mkdir -p /tmp/mnt/%s\n" % pvc.relpath
                    new_template.script += "cp -r %s /tmp/mnt/%s/%s\n" % (art.path, pvc.relpath, pvc.name)
            elif (isinstance(new_template, PythonScriptOPTemplate)):
                new_template.script += "\nimport os\n"
                for pvc, art in pvc_arts:
                    new_template.mounts.append(V1VolumeMount(name=pvc.pvcname, mount_path="/tmp/mnt"))
                    new_template.script += "os.system('mkdir -p /tmp/mnt/%s')\n" % pvc.relpath
                    new_template.script += "os.system('cp -r %s /tmp/mnt/%s/%s')\n" % (art.path, pvc.relpath, pvc.name)
            else:
                raise RuntimeError("Unsupported type of OPTemplate to mount PVC")

        if self.continue_on_num_success  or self.continue_on_success_ratio is not None:
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

        if isinstance(self.with_param, (InputParameter, OutputParameter)):
            self.with_param = "{{%s}}" % self.with_param

        return V1alpha1WorkflowStep(
            name=self.name, template=self.template.name, arguments=V1alpha1Arguments(
                parameters=argo_parameters,
                artifacts=argo_artifacts
            ), when=self.when, with_param=self.with_param, continue_on=V1alpha1ContinueOn(failed=self.continue_on_failed)
        )

    def run(self, context):
        import os
        import shutil
        import uuid
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

        workdir = self.name + str(uuid.uuid4())
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
