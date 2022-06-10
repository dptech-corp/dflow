import copy
import yaml
from .io import InputArtifact, InputParameter, OutputArtifact, OutputParameter, PVC
from .op_template import ScriptOPTemplate, ShellOPTemplate
from .step import Step
from .steps import Steps
from .executor import Executor, RemoteExecutor
from .resource import Resource
from argo.workflows.client import (
    V1HostPathVolumeSource,
    V1Volume,
    V1VolumeMount,
    V1alpha1ResourceTemplate
)
import re

class SlurmJob(Resource):
    def __init__(self, header="", node_selector=None, prepare=None, results=None):
        self.header = header
        self.action = "create"
        self.success_condition = "status.status == Succeeded"
        self.failure_condition = "status.status == Failed"
        self.node_selector = node_selector
        self.prepare = prepare
        self.results = results

    def get_manifest(self, command, script, workdir="."):
        manifest = {
            "apiVersion": "wlm.sylabs.io/v1alpha1",
            "kind": "SlurmJob",
            "metadata": {
                "name": "{{pod.name}}"
            },
            "spec": {
                "batch": self.header + "\nmkdir -p %s\ncd %s\ncat <<EOF | %s\n%s\nEOF" % (workdir, workdir, " ".join(command), script)
            }
        }
        if self.node_selector is not None:
            manifest["spec"]["nodeSelector"] = self.node_selector
        if self.prepare is not None:
            manifest["spec"]["prepare"] = self.prepare
        if self.results is not None:
            manifest["spec"]["results"] = self.results
        return yaml.dump(manifest, default_style="|")

class SlurmJobTemplate(Executor):
    """
    Slurm job template

    Args:
        header: header for Slurm job
        node_selector: node selector
        prepare_image: image for preparing data
        collect_image: image for collecting results
        workdir: remote working directory
        remote_command: command for running the script remotely
    """
    def __init__(self, header="", node_selector=None, prepare_image="dptechnology/dflow", collect_image="dptechnology/dflow",
            workdir="dflow/workflows/{{workflow.name}}/{{pod.name}}", remote_command=None):
        self.header = header
        self.node_selector = node_selector
        self.prepare_image = prepare_image
        self.collect_image = collect_image
        self.workdir = workdir
        self.remote_command = remote_command

    def render(self, template):
        new_template = Steps(template.name + "-slurm")
        for art_name in template.inputs.artifacts:
            new_template.inputs.artifacts[art_name] = InputArtifact(name=art_name)
        for par_name in template.inputs.parameters:
            new_template.inputs.parameters[par_name] = InputParameter(name=par_name)
        prepare = None
        results = None

        # With using host path here, care should be taken for which node the pod scheduled to
        if template.inputs.artifacts:
            volume = V1Volume(name="workdir", host_path=V1HostPathVolumeSource(path="/tmp/{{pod.name}}", type="DirectoryOrCreate"))
            mount = V1VolumeMount(name="workdir", mount_path="/workdir")
            script = ""
            for art in template.inputs.artifacts.values():
                script += "cp --path -r %s /workdir\n" % art.path
            prepare_template = ShellOPTemplate(name=new_template.name + "-prepare", image=self.prepare_image, script=script, volumes=[volume], mounts=[mount])
            prepare_template.inputs.artifacts = copy.deepcopy(template.inputs.artifacts)
            artifacts = {}
            for art_name in template.inputs.artifacts:
                artifacts[art_name] = new_template.inputs.artifacts[art_name]
            prepare_step = Step("slurm-prepare", template=prepare_template, artifacts=artifacts)
            new_template.add(prepare_step)

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
        command = template.command if self.remote_command is None else self.remote_command
        script = template.script.replace("/tmp", "tmp")
        run_template = ScriptOPTemplate(name=new_template.name + "-run", resource=V1alpha1ResourceTemplate(action=slurm_job.action,
                success_condition=slurm_job.success_condition, failure_condition=slurm_job.failure_condition,
                manifest=slurm_job.get_manifest(command=command, script=script, workdir="%s/workdir" % self.workdir)))
        run_template.inputs.parameters = copy.deepcopy(template.inputs.parameters)
        parameters = {}
        for par_name in template.inputs.parameters:
            parameters[par_name] = "{{inputs.parameters.%s}}" % par_name
        if prepare:
            run_template.inputs.parameters["dflow_vol_path"] = InputParameter()
            parameters["dflow_vol_path"] = "/tmp/{{steps.slurm-prepare.id}}"
        run_step = Step("slurm-run", template=run_template, parameters=parameters)
        new_template.add(run_step)

        if results:
            volume = V1Volume(name="mnt", host_path=V1HostPathVolumeSource(path="{{inputs.parameters.dflow_vol_path}}", type="DirectoryOrCreate"))
            mount = V1VolumeMount(name="mnt", mount_path="/mnt")
            script = ""
            for art in template.outputs.artifacts.values():
                script += "mkdir -p `dirname %s` && cp -r /mnt/workdir/%s %s\n" % (art.path, art.path, art.path)
            for par in template.outputs.parameters.values():
                script += "mkdir -p `dirname %s` && cp -r /mnt/workdir/%s %s\n" % (par.value_from_path, par.value_from_path, par.value_from_path)
            collect_template = ShellOPTemplate(name=new_template.name + "-collect", image=self.collect_image, script=script, volumes=[volume], mounts=[mount])
            collect_template.inputs.parameters["dflow_vol_path"] = InputParameter()
            if "dflow_group_key" in template.inputs.parameters:
                collect_template.inputs.parameters["dflow_group_key"] = InputParameter(value="{{inputs.parameters.dflow_group_key}}")
            collect_template.outputs.parameters = copy.deepcopy(template.outputs.parameters)
            collect_template.outputs.artifacts = copy.deepcopy(template.outputs.artifacts)
            collect_step = Step("slurm-collect", template=collect_template, parameters={"dflow_vol_path": "/tmp/{{steps.slurm-run.id}}"})
            new_template.add(collect_step)

            for art_name in template.outputs.artifacts:
                new_template.outputs.artifacts[art_name] = OutputArtifact(name=art_name, _from="{{steps.slurm-collect.outputs.artifacts.%s}}" % art_name)
            for par_name in template.outputs.parameters:
                new_template.outputs.parameters[par_name] = OutputParameter(name=par_name, value_from_parameter="{{steps.slurm-collect.outputs.parameters.%s}}" % par_name)

        return new_template

class SlurmRemoteExecutor(RemoteExecutor):
    """
    Slurm remote executor

    Args:
        host: remote host
        port: SSH port
        username: username
        password: password for SSH
        private_key_file: private key file for SSH
        workdir: remote working directory
        command: command for the executor
        remote_command: command for running the script remotely
        image: image for the executor
        map_tmp_dir: map /tmp to ./tmp
        docker_executable: docker executable to run remotely
        action_retries: retries for actions (upload, execute commands, download), -1 for infinity
        header: header for Slurm job
        interval: query interval for Slurm
    """
    def __init__(self, host, port=22, username="root", password=None, private_key_file=None, workdir="~/dflow/workflows/{{workflow.name}}/{{pod.name}}",
            command=None, remote_command=None, image="dptechnology/dflow-extender", map_tmp_dir=True, docker_executable=None, action_retries=-1,
            header="", interval=3, pvc=None, size="1Gi", storage_class=None, access_modes=None):
        super().__init__(host=host, port=port, username=username, password=password, private_key_file=private_key_file, workdir=workdir, command=command,
                remote_command=remote_command, image=image, map_tmp_dir=map_tmp_dir, docker_executable=docker_executable, action_retries=action_retries)
        self.header = re.sub(" *#","#",header)
        self.interval = interval
        self.pvc = pvc
        self.size = size
        self.storage_class = storage_class
        self.access_modes = access_modes

    def run(self, image):
        script = ""
        if self.docker_executable is None:
            script += "echo '%s\n%s script' > slurm.sh\n" % (self.header, " ".join(self.remote_command))
        else:
            script += "echo '%s\n%s run -v$(pwd)/tmp:/tmp -v$(pwd)/script:/script -ti %s %s /script' > slurm.sh\n" % (self.header, self.docker_executable, image, " ".join(self.remote_command))
        script += self.upload("slurm.sh", "%s/slurm.sh" % self.workdir) + " || exit 1\n"
        if self.pvc:
            script += "echo 'jobIdFile: /mnt/job_id.txt' >> param.yaml\n"
        else:
            script += "echo 'jobIdFile: /tmp/job_id.txt' >> param.yaml\n"
        script += "echo 'workdir: %s' >> param.yaml\n" % self.workdir
        script += "echo 'scriptFile: slurm.sh' >> param.yaml\n"
        script += "echo 'interval: %s' >> param.yaml\n" % self.interval
        script += "echo 'host: %s' >> param.yaml\n" % self.host
        script += "echo 'port: %s' >> param.yaml\n" % self.port
        script += "echo 'username: %s' >> param.yaml\n" % self.username
        if self.password is not None:
            script += "echo 'password: %s' >> param.yaml\n" % self.password
        else:
            script += "echo 'privateKeyFile: /root/.ssh/id_rsa' >> param.yaml\n"
        script += "./bin/slurm param.yaml || exit 1\n"
        return script

    def render(self, template):
        new_template = super().render(template)
        if self.pvc is not None:
            new_template.pvcs.append(PVC(self.pvc, ".", size=self.size, storage_class=self.storage_class, access_modes=self.access_modes))
            new_template.mounts.append(V1VolumeMount(name=self.pvc, mount_path="/mnt", sub_path="{{pod.name}}"))
        return new_template
