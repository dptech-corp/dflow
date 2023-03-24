import json
import os
from copy import deepcopy
from typing import List, Optional, Union

from ..common import S3Artifact
from ..config import config
from ..executor import Executor, render_script_with_tmp_root, run_script
from ..io import InputArtifact, InputParameter
from ..op_template import ScriptOPTemplate
from ..utils import randstr, upload_s3
from . import bohrium

try:
    from argo.workflows.client import (V1HostPathVolumeSource, V1Volume,
                                       V1VolumeMount)
except Exception:
    pass


def update_dict(d1: dict, d2: dict) -> None:
    for k, v in d2.items():
        if isinstance(v, dict) and k in d1 and isinstance(d1[k], dict):
            update_dict(d1[k], v)
        else:
            d1[k] = v


class DispatcherExecutor(Executor):
    """
    Dispatcher executor

    Args:
        host: remote host
        queue_name: queue name
        port: SSH port
        username: username
        password: password
        private_key_file: private key file for SSH
        image: image for dispatcher
        image_pull_policy: image pull policy for dispatcher
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
        remote_root: remote root path for working
        retry_on_submission_error: max retries on submission error
        merge_sliced_step: handle multi slices in one dispatcher job
    """

    def __init__(self,
                 host: Optional[str] = None,
                 queue_name: Optional[str] = None,
                 port: int = 22,
                 username: str = "root",
                 password: Optional[str] = None,
                 private_key_file: Optional[os.PathLike] = None,
                 image: Optional[str] = None,
                 image_pull_policy: Optional[str] = None,
                 command: Union[str, List[str]] = "python3",
                 remote_command: Union[str, List[str]] = None,
                 map_tmp_dir: bool = True,
                 machine_dict: Optional[dict] = None,
                 resources_dict: Optional[dict] = None,
                 task_dict: Optional[dict] = None,
                 json_file: Optional[os.PathLike] = None,
                 docker_executable: Optional[str] = None,
                 singularity_executable: Optional[str] = None,
                 podman_executable: Optional[str] = None,
                 remote_root: Optional[str] = None,
                 retry_on_submission_error: Optional[int] = None,
                 merge_sliced_step: bool = False,
                 ) -> None:
        self.host = host
        self.queue_name = queue_name
        self.port = port
        self.username = username
        self.password = password
        self.private_key_file = private_key_file
        if image is None:
            image = config["dispatcher_image"]
        self.image = image
        if image_pull_policy is None:
            image_pull_policy = config["dispatcher_image_pull_policy"]
        self.image_pull_policy = image_pull_policy
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
        if config["mode"] == "debug":
            self.work_root = "."
        else:
            self.work_root = "/"
        self.remote_root = remote_root
        self.retry_on_submission_error = retry_on_submission_error
        self.merge_sliced_step = merge_sliced_step

        conf = {}
        if json_file is not None:
            with open(json_file, "r") as f:
                conf = json.load(f)

        self.machine_dict = {
            "batch_type": "Slurm",
            "context_type": "SSHContext",
            "local_root": self.work_root,
            "remote_profile": {
                "hostname": self.host,
                "username": self.username,
                "port": self.port,
                "timeout": 10
            }
        }
        if self.password is not None:
            self.machine_dict["remote_profile"]["password"] = self.password
        if self.remote_root is not None:
            self.machine_dict["remote_root"] = self.remote_root
        else:
            self.machine_dict["remote_root"] = "/home/%s/dflow/workflows" % \
                self.username
        if self.private_key_file is not None:
            self.machine_dict["remote_profile"]["key_filename"] = \
                "/root/.ssh/" + os.path.basename(self.private_key_file)
        if "machine" in conf:
            update_dict(self.machine_dict, conf["machine"])
        if machine_dict is not None:
            update_dict(self.machine_dict, machine_dict)

        if self.machine_dict["context_type"] == "Bohrium":
            if "batch_type" not in self.machine_dict:
                self.machine_dict["batch_type"] = "Bohrium"
            if "email" not in self.machine_dict["remote_profile"] and \
                    bohrium.config["username"] is not None:
                self.machine_dict["remote_profile"]["email"] = \
                    bohrium.config["username"]
            if "password" not in self.machine_dict["remote_profile"] and \
                    bohrium.config["password"] is not None:
                self.machine_dict["remote_profile"]["password"] = \
                    bohrium.config["password"]
            if "program_id" not in self.machine_dict["remote_profile"] and \
                    bohrium.config["project_id"] is not None:
                self.machine_dict["remote_profile"]["program_id"] = int(
                    bohrium.config["project_id"])
            if "input_data" not in self.machine_dict["remote_profile"]:
                self.machine_dict["remote_profile"]["input_data"] = {}
            input_data = self.machine_dict["remote_profile"]["input_data"]
            input_data["log_file"] = "log"
            if "job_type" not in input_data:
                input_data["job_type"] = "container"
            if "platform" not in input_data:
                input_data["platform"] = "ali"
            if "scass_type" not in input_data:
                input_data["scass_type"] = "c4_m8_cpu"
            if "job_name" not in input_data:
                input_data["job_name"] = "{{pod.name}}"
            if "output_log" not in input_data:
                input_data["output_log"] = True

        # set env to prevent dispatcher from considering different tasks as one
        self.resources_dict = {
            "number_node": 1,
            "cpu_per_node": 1,
            "group_size": 5,
            "envs": {
                "DFLOW_WORKFLOW": "{{workflow.name}}",
                "DFLOW_POD": "{{pod.name}}"
            }
        }
        if self.queue_name is not None:
            self.resources_dict["queue_name"] = self.queue_name
        if "resources" in conf:
            update_dict(self.resources_dict, conf["resources"])
        if resources_dict is not None:
            update_dict(self.resources_dict, resources_dict)

        self.task_dict = {
            "task_work_path": "./",
            "outlog": "log",
            "errlog": "log"
        }
        if task_dict is not None:
            update_dict(self.task_dict, task_dict)

    def render(self, template):
        if not isinstance(template, ScriptOPTemplate):
            return template

        new_template = deepcopy(template)
        new_template.name += "-" + randstr()
        new_template.image = self.image
        new_template.image_pull_policy = self.image_pull_policy
        new_template.command = self.command

        remote_command = template.command if self.remote_command is None \
            else self.remote_command
        cmd = ""
        if self.map_tmp_dir:
            cmd += "sed -i \\\"s#\\\\$(pwd)#$(pwd)#g\\\" script && "

        cmd += run_script(template.image, remote_command,
                          self.docker_executable, self.singularity_executable,
                          self.podman_executable)
        self.task_dict["command"] = cmd
        self.task_dict["forward_files"] = ["script"]
        for art in template.inputs.artifacts.values():
            self.task_dict["forward_files"].append("./" + art.path)
        for par in template.inputs.parameters.values():
            if par.save_as_artifact:
                self.task_dict["forward_files"].append("./" + par.path)
        merge = self.merge_sliced_step and hasattr(template, "slices") and\
            template.slices is not None
        if merge:
            sliced_output_parameters = template.slices.output_parameter.copy()
            if "dflow_success_tag" in template.outputs.parameters:
                sliced_output_parameters.append("dflow_success_tag")
        self.task_dict["backward_files"] = []
        for art in template.outputs.artifacts.values():
            self.task_dict["backward_files"].append("./" + art.path)
        for name, par in template.outputs.parameters.items():
            if par.save_as_artifact:
                self.task_dict["backward_files"].append(
                    "./" + par.value_from_path)
            elif par.value_from_path is not None and not (
                    merge and sliced_output_parameters):
                self.task_dict["backward_files"].append(
                    "./" + par.value_from_path)

        new_template.script = "import os\n"
        new_template.script += "os.chdir('%s')\n" % self.work_root
        new_template.script += "with open('script', 'w') as f:\n"
        new_template.script += "    f.write(r\"\"\"\n"
        if self.map_tmp_dir:
            new_template.script += render_script_with_tmp_root(template,
                                                               "$(pwd)/tmp")
        else:
            new_template.script += template.script
        new_template.script += "\"\"\")\n"

        if self.machine_dict["context_type"] == "Bohrium":
            if "image_name" not in self.machine_dict["remote_profile"][
                    "input_data"]:
                self.machine_dict["remote_profile"]["input_data"][
                    "image_name"] = template.image

        self.machine_dict["local_root"] = self.work_root
        new_template.script += "import json, shlex\n"
        new_template.script += "from dpdispatcher import Machine, Resources,"\
            " Task, Submission\n"
        new_template.script += "machine = Machine.load_from_dict(json.loads("\
            "'%s'))\n" % json.dumps(self.machine_dict)
        new_template.script += "resources = Resources.load_from_dict(json."\
            "loads('%s'))\n" % json.dumps(self.resources_dict)
        if new_template.envs is not None:
            for k in new_template.envs.keys():
                new_template.script += "resources.envs['%s'] = "\
                    "os.environ.get('%s')\n" % (k, k)
        new_template.script += "resources.envs['ARGO_TEMPLATE'] = "\
            "shlex.quote(os.environ.get('ARGO_TEMPLATE'))\n"
        new_template.script += "task = Task.load_from_dict(json.loads('%s'))"\
            "\n" % json.dumps(self.task_dict)
        new_template.script += "task.forward_files = list(filter("\
            "os.path.exists, task.forward_files))\n"
        new_template.script += "for f in task.backward_files:\n"
        new_template.script += "    os.makedirs(os.path.dirname(f), "\
            "exist_ok=True)\n"
        if merge:
            new_template.inputs.parameters["dflow_with_param"] = \
                InputParameter(value="")
            new_template.inputs.parameters["dflow_sequence_start"] = \
                InputParameter(value=0)
            new_template.inputs.parameters["dflow_sequence_end"] = \
                InputParameter(value=None)
            new_template.inputs.parameters["dflow_sequence_count"] = \
                InputParameter(value=None)
            new_template.inputs.parameters["dflow_sequence_format"] = \
                InputParameter(value="")
            new_template.script += "from copy import deepcopy\n"
            new_template.script += "with open('script', 'r') as f:\n"
            new_template.script += "    script = f.read()\n"
            new_template.script += "tasks = []\n"
            new_template.script += "with_param = r'''{{inputs.parameters."\
                "dflow_with_param}}'''\n"
            new_template.script += "if with_param != '':\n"
            new_template.script += "    item_list = json.loads(with_param)\n"
            new_template.script += "else:\n"
            new_template.script += "    start = json.loads('{{inputs."\
                "parameters.dflow_sequence_start}}')\n"
            new_template.script += "    count = json.loads('{{inputs."\
                "parameters.dflow_sequence_count}}')\n"
            new_template.script += "    end = json.loads('{{inputs."\
                "parameters.dflow_sequence_end}}')\n"
            new_template.script += "    format = '{{inputs.parameters."\
                "dflow_sequence_format}}'\n"
            new_template.script += "    if count is not None:\n"
            new_template.script += "        r = range(start, start + count)\n"
            new_template.script += "    elif end is not None:\n"
            new_template.script += "        if end > start:\n"
            new_template.script += "            r = range(start, end + 1)\n"
            new_template.script += "        else:\n"
            new_template.script += "            r = range(start, end - 1, -1)"\
                "\n"
            new_template.script += "    item_list = [format % i if format != "\
                "'' else i for i in r]\n"
            new_template.script += "for i, item in enumerate(item_list):\n"
            new_template.script += "    new_task = deepcopy(task)\n"
            new_template.script += "    new_script = script\n"
            for k, v in new_template.dflow_vars.items():
                if "item" in k:
                    old = "'{{inputs.parameters.%s}}'" % v
                    new = "item" if k == "{{item}}" else "item[%s]" % k[6:-2]
                    new_template.script += "    new_script = new_script."\
                        "replace(%s, %s if isinstance(%s, str) else "\
                        "json.dumps(%s))\n" % (old, new, new, new)
            for name in template.slices.output_parameter:
                new_template.script += "    new_script = new_script.replace("\
                    "\"handle_output_parameter('%s'\", "\
                    "\"handle_output_parameter('%s_\" + str(i) + \"'\")\n" % (
                        name, name)
                path = template.outputs.parameters[name].value_from_path
                new_template.script += "    os.makedirs(os.path.dirname("\
                    "'./%s'), exist_ok=True)\n" % path
                new_template.script += "    new_task.backward_files.append("\
                    "'./%s_' + str(i))\n" % path
            if "dflow_success_tag" in template.outputs.parameters:
                new_template.script += "    new_script = new_script.replace("\
                    "'success_tag', 'success_tag_' + str(i))\n"
                path = template.outputs.parameters[
                    "dflow_success_tag"].value_from_path
                new_template.script += "    os.makedirs(os.path.dirname("\
                    "'./%s'), exist_ok=True)\n" % path
                new_template.script += "    new_task.backward_files.append("\
                    "'./%s_' + str(i))\n" % path
            new_template.script += "    with open('script' + str(i), 'w')"\
                " as f:\n"
            new_template.script += "        f.write(new_script)\n"
            new_template.script += "    new_task.command = new_task.command."\
                "replace('script', 'script' + str(i))\n"
            new_template.script += "    new_task.forward_files[0] = 'script'"\
                " + str(i)\n"
            new_template.script += "    tasks.append(new_task)\n"
            new_template.script += "resources.group_size = 1\n"
            new_template.script += "submission = Submission(work_base='.', "\
                "machine=machine, resources=resources, task_list=tasks)\n"
        else:
            new_template.script += "submission = Submission(work_base='.', "\
                "machine=machine, resources=resources, task_list=[task])\n"
        if self.retry_on_submission_error:
            new_template.script += "for retry in range(%s):\n" % \
                self.retry_on_submission_error
            new_template.script += "    try:\n"
            new_template.script += "        print('retry ' + str(retry))\n"
            new_template.script += "        submission.run_submission()\n"
            new_template.script += "        break\n"
            new_template.script += "    except Exception:\n"
            new_template.script += "        import traceback\n"
            new_template.script += "        traceback.print_exc()\n"
            new_template.script += "        import time\n"
            new_template.script += "        time.sleep(2**retry)\n"
        else:
            new_template.script += "submission.run_submission()\n"
        if merge:
            for name in sliced_output_parameters:
                path = template.outputs.parameters[name].value_from_path
                new_template.script += "res = []\n"
                new_template.script += "for i in range(len(item_list)):\n"
                new_template.script += "    fname = './%s_' + str(i)\n" % path
                new_template.script += "    if os.path.isfile(fname):\n"
                new_template.script += "        with open(fname, 'r') as f:\n"
                new_template.script += "            res.append(f.read())\n"
                new_template.script += "with open('./%s', 'w') as f:\n" % path
                new_template.script += "    f.write(json.dumps(res))\n"

        # workaround for unavailable exit code of Bohrium job
        # check output files explicitly
        for art in template.outputs.artifacts.values():
            new_template.script += "assert os.path.exists('./%s')\n" % art.path
        for par in template.outputs.parameters.values():
            if par.save_as_artifact or (par.value_from_path is not None and
                                        not hasattr(par, "default")):
                new_template.script += "assert os.path.exists('./%s')\n" % \
                    par.value_from_path

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
