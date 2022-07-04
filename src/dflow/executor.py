import os
from copy import deepcopy
from typing import List, Union

from .common import S3Artifact
from .config import config
from .io import InputArtifact
from .op_template import OPTemplate
from .utils import randstr, upload_s3

try:
    from argo.workflows.client import (V1HostPathVolumeSource, V1Volume,
                                       V1VolumeMount)
except:
    pass

class Executor(object):
    """
    Executor
    """
    def render(
            self,
            template : OPTemplate,
    ) -> OPTemplate:
        """
        render original template and return a new template, do not modify self in this method to make the executor reusable
        """
        raise NotImplementedError()

class RemoteExecutor(Executor):
    def __init__(
            self,
            host : str,
            port : int = 22,
            username : str = "root",
            password : str = None,
            private_key_file : os.PathLike = None,
            workdir : str = "~/dflow/workflows/{{workflow.name}}/{{pod.name}}",
            command : Union[str, List[str]] = None,
            remote_command : Union[str, List[str]] = None,
            image : str = "dptechnology/dflow-extender",
            map_tmp_dir : bool = True,
            docker_executable : str = None,
            action_retries : int = -1,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.private_key_file = private_key_file
        self.workdir = workdir
        if isinstance(command, str):
            command = [command]
        if command is None:
            command = ["sh"]
        self.command = command
        if isinstance(remote_command, str):
            remote_command = [remote_command]
        self.remote_command = remote_command
        self.image = image
        self.map_tmp_dir = map_tmp_dir
        self.docker_executable = docker_executable
        if self.docker_executable is not None:
            self.map_tmp_dir = False
        self.action_retries = action_retries

    def execute(self, cmd):
        return "execute %s '%s'" % (self.action_retries, cmd) # add '' in case shell will expand ~

    def upload(self, src, dst):
        return "upload %s '%s' '%s'" % (self.action_retries, src, dst) # add '' in case shell will expand ~

    def download(self, src, dst):
        return "download %s '%s' '%s'" % (self.action_retries, src, dst) # add '' in case shell will expand ~

    def run(self, image, remote_command):
        if self.docker_executable is None:
            map_cmd = " && sed -i \"s#/tmp#$(pwd)/tmp#g\" script" if self.map_tmp_dir else ""
            return self.execute("cd %s %s && %s script" % (self.workdir, map_cmd, " ".join(self.remote_command))) + " || exit 1\n"
        else:
            return self.execute("cd %s && %s run -v$(pwd)/tmp:/tmp -v$(pwd)/script:/script -ti %s %s /script" % (self.workdir, self.docker_executable, image, " ".join(remote_command))) + " || exit 1\n"

    def mkdir_and_upload(self, path):
        return self.execute("mkdir -p %s/%s" % (self.workdir, os.path.dirname(path))) + "\n" + \
                ("if [ -e %s ]; then " % path) + self.upload(path, "%s/%s" % (self.workdir, path)) + "; fi\n"

    def mkdir_and_download(self, path):
        return "mkdir -p %s" % os.path.dirname(path) + "\n" + \
                self.download("%s/%s" % (self.workdir, path), path) + "\n"

    def get_script(self, template):
        remote_script = template.script
        remote_command = template.command if self.remote_command is None else self.remote_command
        ssh_pass = "sshpass -p %s " % self.password if self.password is not None else ""
        script = """
execute() {
    if [ $1 != 0 ]; then
        %sssh -C -o StrictHostKeyChecking=no -p %s %s@%s -- $2
        if [ $? != 0 ]; then
            echo retry: $1
            sleep 1
            execute $(($1-1)) '$2'
        fi
    fi
}
""" % (ssh_pass, self.port, self.username, self.host)
        script += """
upload() {
    if [ $1 != 0 ]; then
        %sscp -C -o StrictHostKeyChecking=no -P %s -r $2 %s@%s:$3
        if [ $? != 0 ]; then
            echo retry: $1
            sleep 1
            upload $(($1-1)) $2 $3
        fi
    fi
}
""" % (ssh_pass, self.port, self.username, self.host)
        script += """
download() {
    if [ $1 != 0 ]; then
        %sscp -C -o StrictHostKeyChecking=no -P %s -r %s@%s:$2 $3
        if [ $? != 0 ]; then
            echo retry: $1
            sleep 1
            download $(($1-1)) $2 $3
        fi
    fi
}
""" % (ssh_pass, self.port, self.username, self.host)
        script += "cat <<EOF> script\n" + remote_script + "\nEOF\n"
        for art in template.inputs.artifacts.values():
            script += self.mkdir_and_upload(art.path)
        for par in template.inputs.parameters.values():
            if par.save_as_artifact:
                script += self.mkdir_and_upload(par.path)
        script += self.mkdir_and_upload("script")
        script += self.run(template.image, remote_command)
        for art in template.outputs.artifacts.values():
            script += self.mkdir_and_download(art.path)
        for par in template.outputs.parameters.values():
            if par.save_as_artifact:
                script += self.mkdir_and_download(par.path)
            elif par.value_from_path is not None:
                script += self.mkdir_and_download(par.value_from_path)
        return script

    def render(self, template):
        new_template = deepcopy(template)
        new_template.image = self.image
        new_template.command = self.command
        new_template.script = self.get_script(template)
        new_template.name += "-" + randstr()
        if self.password is not None:
            pass
        elif self.private_key_file is not None:
            key = upload_s3(self.private_key_file)
            private_key_artifact = S3Artifact(key=key)
            new_template.inputs.artifacts["dflow_private_key"] = InputArtifact(path="/root/.ssh/" + os.path.basename(self.private_key_file), source=private_key_artifact, mode=0o600)
        else:
            new_template.volumes.append(V1Volume(name="dflow-private-key", host_path=V1HostPathVolumeSource(path=config["private_key_host_path"])))
            new_template.mounts.append(V1VolumeMount(name="dflow-private-key", mount_path="/root/.ssh"))
        return new_template
