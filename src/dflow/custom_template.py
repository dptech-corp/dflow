import copy
from .io import InputArtifact, InputParameter, OutputArtifact, OutputParameter
from .op_template import ScriptOPTemplate, ShellOPTemplate
from .resource import SlurmJob
from .step import Step
from .steps import Steps
from argo.workflows.client import (
    V1HostPathVolumeSource,
    V1Volume,
    V1VolumeMount,
    V1alpha1ResourceTemplate
)

class SlurmJobTemplate(Steps):
    def __init__(self, header="", node_selector=None, prepare_image="dptechnology/dflow", collect_image="dptechnology/dflow",
            workdir="dflow/workflows/{{workflow.name}}/{{pod.name}}", remote_command=None):
        super().__init__("slurm-job")
        self.header = header
        self.node_selector = node_selector
        self.prepare_image = prepare_image
        self.collect_image = collect_image
        self.workdir = workdir
        self.remote_command = remote_command

    def render(self, template):
        self.name = template.name + "-slurm"
        for art_name in template.inputs.artifacts:
            self.inputs.artifacts[art_name] = InputArtifact(name=art_name)
        for par_name in template.inputs.parameters:
            self.inputs.parameters[par_name] = InputParameter(name=par_name)
        prepare = {}
        results = {}

        # With using host path here, care should be taken for which node the pod scheduled to
        if template.inputs.artifacts:
            volume = V1Volume(name="workdir", host_path=V1HostPathVolumeSource(path="/tmp/{{pod.name}}", type="DirectoryOrCreate"))
            mount = V1VolumeMount(name="workdir", mount_path="/workdir")
            script = ""
            for art in template.inputs.artifacts.values():
                script += "cp --path -r %s /workdir\n" % art.path
            prepare_template = ShellOPTemplate(name=self.name + "-prepare", image=self.prepare_image, script=script, volumes=[volume], mounts=[mount])
            prepare_template.inputs.artifacts = copy.deepcopy(template.inputs.artifacts)
            artifacts = {}
            for art_name in template.inputs.artifacts:
                artifacts[art_name] = self.inputs.artifacts[art_name]
            prepare_step = Step(name=self.name + "-prepare", template=prepare_template, artifacts=artifacts)
            self.add(prepare_step)

            prepare = {
                "to": self.workdir,
                "mount": {
                    "name": "workdir",
                    "hostPath": {
                        "path": "{{inputs.parameters.dflow_vol_path}}",
                        "type": "DirectoryOrCreate"
                    }
                }
            }

        if template.outputs.parameters or template.outputs.artifacts:
            results = {
                "from": "%s/workdir" % self.workdir,
                "mount": {
                    "name": "mnt",
                    "hostPath": {
                        "path": "/tmp/{{pod.name}}",
                        "type": "DirectoryOrCreate"
                    }
                }
            }

        slurm_job = SlurmJob(header=self.header, node_selector=self.node_selector, prepare=prepare, results=results)
        if self.remote_command is None:
            self.remote_command = template.command
        script = template.script.replace("/tmp", "tmp")
        run_template = ScriptOPTemplate(name=self.name + "-run", resource=V1alpha1ResourceTemplate(action=slurm_job.action,
                success_condition=slurm_job.success_condition, failure_condition=slurm_job.failure_condition,
                manifest=slurm_job.get_manifest(command=self.remote_command, script=script, workdir="%s/workdir" % self.workdir)))
        run_template.inputs.parameters = copy.deepcopy(template.inputs.parameters)
        parameters = {}
        for par_name in template.inputs.parameters:
            parameters[par_name] = "{{inputs.parameters.%s}}" % par_name
        if prepare:
            run_template.inputs.parameters["dflow_vol_path"] = InputParameter()
            parameters["dflow_vol_path"] = "/tmp/{{steps.%s-prepare.id}}" % self.name
        run_step = Step(name=self.name + "-run", template=run_template, parameters=parameters)
        self.add(run_step)

        if results:
            volume = V1Volume(name="mnt", host_path=V1HostPathVolumeSource(path="{{inputs.parameters.dflow_vol_path}}", type="DirectoryOrCreate"))
            mount = V1VolumeMount(name="mnt", mount_path="/mnt")
            script = ""
            for art in template.outputs.artifacts.values():
                script += "mkdir -p `dirname %s` && cp -r /mnt/workdir/%s %s\n" % (art.path, art.path, art.path)
            for par in template.outputs.parameters.values():
                script += "mkdir -p `dirname %s` && cp -r /mnt/workdir/%s %s\n" % (par.value_from_path, par.value_from_path, par.value_from_path)
            collect_template = ShellOPTemplate(name=self.name + "-collect", image=self.collect_image, script=script, volumes=[volume], mounts=[mount])
            collect_template.inputs.parameters["dflow_vol_path"] = InputParameter()
            collect_template.outputs.parameters = copy.deepcopy(template.outputs.parameters)
            collect_template.outputs.artifacts = copy.deepcopy(template.outputs.artifacts)
            collect_step = Step(name=self.name + "-collect", template=collect_template, parameters={"dflow_vol_path": "/tmp/{{steps.%s-run.id}}" % self.name})
            self.add(collect_step)

            for art_name in template.outputs.artifacts:
                self.outputs.artifacts[art_name] = OutputArtifact(name=art_name, _from="{{steps.%s-collect.outputs.artifacts.%s}}" % (self.name, art_name))
            for par_name in template.outputs.parameters:
                self.outputs.parameters[par_name] = OutputParameter(name=par_name, value_from_parameter="{{steps.%s-collect.outputs.parameters.%s}}" % (self.name, par_name))
