from .common import S3Artifact
from .io import InputArtifact
from .utils import upload_s3
from .op_template import ShellOPTemplate

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
            command=None, remote_command=None, image="dptechnology/dflow-extender", map_tmp_dir=True, container=None):
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
        self.container = container
        if self.container is not None:
            self.map_tmp_dir = False

    def execute(self, cmd):
        if self.password is None:
            return "ssh -o StrictHostKeyChecking=no -p %s %s@%s '%s'" % (self.port, self.username, self.host, cmd)
        else:
            return "sshpass -p %s ssh -o StrictHostKeyChecking=no -p %s %s@%s '%s'" % (self.password, self.port, self.username, self.host, cmd)

    def upload(self, src, dst):
        if self.password is None:
            return "scp -o StrictHostKeyChecking=no -P %s -r %s %s@%s:%s" % (self.port, src, self.username, self.host, dst)
        else:
            return "sshpass -p %s scp -o StrictHostKeyChecking=no -P %s -r %s %s@%s:%s" % (self.password, self.port, src, self.username, self.host, dst)

    def download(self, src, dst):
        if self.password is None:
            return "scp -o StrictHostKeyChecking=no -P %s -r %s@%s:%s %s" % (self.port, self.username, self.host, src, dst)
        else:
            return "sshpass -p %s scp -o StrictHostKeyChecking=no -P %s -r %s@%s:%s %s" % (self.password, self.port, self.username, self.host, src, dst)  

    def run(self):
        if self.container is None:
            return self.execute("cd %s && %s script" % (self.workdir, " ".join(self.remote_command))) + " || exit 1\n"
        else:
            return self.execute("cd %s && %s run -v$(pwd)/tmp:/tmp -v$(pwd)/script:/script -ti %s %s /script" % (self.workdir, self.container, image, " ".join(self.remote_command))) + " || exit 1\n"

    def get_script(self, command, script, image):
        if self.remote_command is None:
            self.remote_command = command
        script = "cat <<EOF> script\n" + (script.replace("/tmp", "tmp") if self.map_tmp_dir else script) + "\nEOF\n"
        script += self.execute("mkdir -p %s/tmp" % self.workdir) + " || exit 1\n"
        script += "if [ -d /tmp ]; then " + self.upload("/tmp", self.workdir) + " || exit 1; fi\n"
        script += self.upload("script", "%s/script" % self.workdir) + " || exit 1\n"
        script += self.run(image)
        script += self.download("%s/tmp/*" % self.workdir, "/tmp") + " || exit 1\n"
        return script

    def render(self, template):
        new_template = ShellOPTemplate(name=template.name + "-remote", inputs=template.inputs, outputs=template.outputs,
                        image=self.image, command=self.command, script=self.get_script(template.command, template.script, template.image),
                        volumes=template.volumes, mounts=template.mounts, init_progress=template.init_progress,
                        timeout=template.timeout, retry_strategy=template.retry_strategy, memoize_key=template.memoize_key)
        if self.private_key_file is not None:
            key = upload_s3(self.private_key_file)
            private_key_artifact = S3Artifact(key=key)
            new_template.inputs.artifacts["dflow_private_key"] = InputArtifact(path="/root/.ssh/id_rsa", source=private_key_artifact)
        return new_template
