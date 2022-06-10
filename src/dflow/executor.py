from copy import deepcopy
from .common import S3Artifact
from .io import InputArtifact
from .utils import upload_s3, randstr
from .op_template import ShellOPTemplate
from .workflow import config
from argo.workflows.client import (
    V1Volume,
    V1VolumeMount,
    V1HostPathVolumeSource
)

class Executor(object):
    """
    Executor
    """
    def render(self, template):
        """
        render original template and return a new template, do not modify self in this method to make the executor reusable
        """
        raise NotImplementedError()

class RemoteExecutor(Executor):
    def __init__(self, host, port=22, username="root", password=None, private_key_file=None, workdir="~/dflow/workflows/{{workflow.name}}/{{pod.name}}",
            command=None, remote_command=None, image="dptechnology/dflow-extender", map_tmp_dir=True, docker_executable=None, action_retries=-1):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.private_key_file = private_key_file
        self.workdir = workdir
        if command is None:
            command = ["sh"]
        self.command = command
        if remote_command is not None and not isinstance(remote_command, list):
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

    def run(self):
        if self.docker_executable is None:
            return self.execute("cd %s && %s script" % (self.workdir, " ".join(self.remote_command))) + " || exit 1\n"
        else:
            return self.execute("cd %s && %s run -v$(pwd)/tmp:/tmp -v$(pwd)/script:/script -ti %s %s /script" % (self.workdir, self.docker_executable, image, " ".join(self.remote_command))) + " || exit 1\n"

    def get_script(self, command, script, image):
        remote_script = script.replace("/tmp", "tmp") if self.map_tmp_dir else script
        if self.remote_command is None:
            self.remote_command = command
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
        script += self.execute("mkdir -p %s/tmp" % self.workdir) + " || exit 1\n"
        script += "if [ -d /tmp ]; then " + self.upload("/tmp", self.workdir) + " || exit 1; fi\n"
        script += self.upload("script", "%s/script" % self.workdir) + " || exit 1\n"
        script += self.run(image)
        script += self.download("%s/tmp/*" % self.workdir, "/tmp") + " || exit 1\n"
        return script

    def render(self, template):
        new_template = deepcopy(template)
        new_template.image = self.image
        new_template.command = self.command
        new_template.script = self.get_script(template.command, template.script, template.image)
        new_template.name += "-" + randstr()
        if self.password is not None:
            pass
        elif self.private_key_file is not None:
            key = upload_s3(self.private_key_file)
            private_key_artifact = S3Artifact(key=key)
            new_template.inputs.artifacts["dflow_private_key"] = InputArtifact(path="/root/.ssh/id_rsa", source=private_key_artifact)
        else:
            new_template.volumes.append(V1Volume(name="dflow-private-key", host_path=V1HostPathVolumeSource(path=config["private_key_host_path"])))
            new_template.mounts.append(V1VolumeMount(name="dflow-private-key", mount_path="/root/.ssh/id_rsa"))
        return new_template
