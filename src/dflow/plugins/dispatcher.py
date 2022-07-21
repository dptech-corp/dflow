import json
import os
from copy import deepcopy
from typing import List, Union

from ..common import S3Artifact
from ..config import config
from ..executor import Executor, run_script
from ..io import InputArtifact
from ..utils import randstr, upload_s3

try:
    from argo.workflows.client import (V1HostPathVolumeSource, V1Volume,
                                       V1VolumeMount)
except Exception:
    pass


class DispatcherExecutor(Executor):
    """
    Dispatcher executor

    Args:
        host: remote host
        queue_name: queue name
        port: SSH port
        username: username
        private_key_file: private key file for SSH
        image: image for dispatcher
        command: command for dispatcher
        remote_command: command for running the script remotely
        map_tmp_dir: map /tmp to ./tmp
        machine_dict: machine config for dispatcher
        resources_dict: resources config for dispatcher
        task_dict: task config for dispatcher
        json_file: JSON file containing machine and resources config
        docker_executable: docker executable to run remotely
        singularity_executable: singularity executable to run remotely
        podman_executable: podman executable to run remotely
    """

    def __init__(self,
                 host: str = None,
                 queue_name: str = None,
                 port: int = 22,
                 username: str = "root",
                 private_key_file: os.PathLike = None,
                 image: str = None,
                 command: Union[str, List[str]] = "python",
                 remote_command: Union[str, List[str]] = None,
                 map_tmp_dir: bool = True,
                 machine_dict: dict = None,
                 resources_dict: dict = None,
                 task_dict: dict = None,
                 json_file: os.PathLike = None,
                 docker_executable: str = None,
                 singularity_executable: str = None,
                 podman_executable: str = None,
                 ) -> None:
        self.host = host
        self.queue_name = queue_name
        self.port = port
        self.username = username
        self.private_key_file = private_key_file
        if image is None:
            image = config["dispatcher_image"]
        self.image = image
        if isinstance(command, str):
            command = [command]
        self.command = command
        if isinstance(remote_command, str):
            remote_command = [remote_command]
        self.remote_command = remote_command
        self.map_tmp_dir = map_tmp_dir
        self.docker_executable = docker_executable
        self.singularity_executable = singularity_executable
        self.podman_executable = podman_executable
        if self.docker_executable is not None or \
                self.singularity_executable is not None or \
                self.podman_executable is not None:
            self.map_tmp_dir = False

        conf = {}
        if json_file is not None:
            with open(json_file, "r") as f:
                conf = json.load(f)

        self.machine_dict = {
            "batch_type": "Slurm",
            "context_type": "SSHContext",
            "local_root": "/",
            "remote_root": "/home/%s/dflow/workflows" % self.username,
            "remote_profile": {
                "hostname": self.host,
                "username": self.username,
                "port": self.port,
                "timeout": 10
            }
        }
        if "machine" in conf:
            self.machine_dict.update(conf["machine"])
        if machine_dict is not None:
            self.machine_dict.update(machine_dict)

        # set env to prevent dispatcher from considering different tasks as one
        self.resources_dict = {
            "number_node": 1,
            "cpu_per_node": 1,
            "gpu_per_node": 1,
            "queue_name": self.queue_name,
            "group_size": 5,
            "envs": {
                "DFLOW_WORKFLOW": "{{workflow.name}}",
                "DFLOW_POD": "{{pod.name}}"
            }
        }
        if "resources" in conf:
            self.resources_dict.update(conf["resources"])
        if resources_dict is not None:
            self.resources_dict.update(resources_dict)

        self.task_dict = {
            "task_work_path": "./",
            "outlog": "log",
            "errlog": "err"
        }
        if task_dict is not None:
            self.task_dict.update(task_dict)

    def render(self, template):
        new_template = deepcopy(template)
        new_template.name += "-" + randstr()
        new_template.image = self.image
        new_template.command = self.command

        remote_command = template.command if self.remote_command is None \
            else self.remote_command
        cmd = ""
        if self.map_tmp_dir:
            cmd += "if [ \\\"$(head -n 1 script)\\\" != \\\"# modified by "\
                "dflow\\\" ]; then sed -i \\\"s#/tmp#$(pwd)/tmp#g\\\" script"\
                "; sed -i \\\"1i # modified by dflow\\\" script; fi && "

        cmd += run_script(template.image, remote_command,
                          self.docker_executable, self.singularity_executable,
                          self.podman_executable)
        self.task_dict["command"] = cmd
        self.task_dict["forward_files"] = ["script"]
        for art in template.inputs.artifacts.values():
            self.task_dict["forward_files"].append(art.path)
        for par in template.inputs.parameters.values():
            if par.save_as_artifact:
                self.task_dict["forward_files"].append(par.path)
        self.task_dict["backward_files"] = []
        for art in template.outputs.artifacts.values():
            self.task_dict["backward_files"].append("./" + art.path)
        for par in template.outputs.parameters.values():
            if par.save_as_artifact:
                self.task_dict["backward_files"].append("./" + par.path)
            elif par.value_from_path is not None:
                self.task_dict["backward_files"].append(
                    "./" + par.value_from_path)

        new_template.script = "import os\n"
        new_template.script += "os.chdir('/')\n"
        new_template.script += "with open('script', 'w') as f:\n"
        new_template.script += "    f.write(r\"\"\"\n"
        new_template.script += template.script
        new_template.script += "\"\"\")\n"

        new_template.script += "import json\n"
        new_template.script += "from dpdispatcher import Machine, Resources,"\
            " Task, Submission\n"
        new_template.script += "machine = Machine.load_from_dict(json.loads("\
            "'%s'))\n" % json.dumps(self.machine_dict)
        new_template.script += "resources = Resources.load_from_dict(json."\
            "loads('%s'))\n" % json.dumps(self.resources_dict)
        new_template.script += "task = Task.load_from_dict(json.loads('%s'))"\
            "\n" % json.dumps(self.task_dict)
        new_template.script += "submission = Submission(work_base='.', "\
            "machine=machine, resources=resources, task_list=[task])\n"
        new_template.script += "submission.run_submission()\n"

        if self.private_key_file is not None:
            key = upload_s3(self.private_key_file)
            private_key_artifact = S3Artifact(key=key)
            new_template.inputs.artifacts["dflow_private_key"] = InputArtifact(
                path="/root/.ssh/" + os.path.basename(self.private_key_file),
                source=private_key_artifact, mode=0o600)
        else:
            new_template.volumes.append(V1Volume(
                name="dflow-private-key", host_path=V1HostPathVolumeSource(
                    path=config["private_key_host_path"])))
            new_template.mounts.append(V1VolumeMount(
                name="dflow-private-key", mount_path="/root/.ssh"))
        return new_template
