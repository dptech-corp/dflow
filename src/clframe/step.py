from copy import deepcopy
from argo.workflows.client import (
    V1alpha1WorkflowStep,
    V1alpha1Arguments,
    V1VolumeMount
)
from .io import PVC
from .op_template import ShellOPTemplate, PythonScriptOPTemplate

class Step:
    def __init__(self, name, template, parameters=None, artifacts=None, when=None):
        self.name = name
        self.id = "steps.%s" % self.name
        self.template = template
        self.inputs = deepcopy(self.template.inputs)
        self.outputs = deepcopy(self.template.outputs)
        self.inputs.set_step_id(self.id)
        self.outputs.set_step_id(self.id)

        if parameters is not None:
            self.set_parameters(parameters)

        if artifacts is not None:
            self.set_artifacts(artifacts)

        self.when = when

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

        if new_template is not None:
            self.template = new_template

        return V1alpha1WorkflowStep(
            name=self.name, template=self.template.name, arguments=V1alpha1Arguments(
                parameters=argo_parameters,
                artifacts=argo_artifacts
            ), when=self.when
        )
