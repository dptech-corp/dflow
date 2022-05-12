class Executor(object):
    """
    Executor
    :param image: the image to execute the script
    :param command: the command to execute the script
    :method get_script: 
    """
    image = None
    command = None
    def get_script(self, command, script):
        raise NotImplementedError()

class RemoteExecutor(Executor):
    def __init__(self, host, port=22, username="root", password=None, workdir="~/dflow/workflows/{{workflow.name}}/{{pod.name}}", command=None, remote_command=None,
            image="dptechnology/dflow-extender"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.workdir = workdir
        if command is None:
            command = ["sh"]
        self.command = command
        self.remote_command = remote_command
        self.image = image

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
        return self.execute("cd %s && %s script" % (self.workdir, " ".join(self.remote_command))) + " || exit 1\n"

    def get_script(self, command, script):
        if self.remote_command is None:
            self.remote_command = command
        script = "cat <<EOF> script\n" + script.replace("/tmp", "tmp") + "\nEOF\n"
        script += self.execute("mkdir -p %s/tmp" % self.workdir) + " || exit 1\n"
        script += "if [ -d /tmp ]; then " + self.upload("/tmp", self.workdir) + " || exit 1; fi\n"
        script += self.upload("script", "%s/script" % self.workdir) + " || exit 1\n"
        script += self.run()
        script += self.download("%s/tmp/*" % self.workdir, "/tmp") + " || exit 1\n"
        return script

class SlurmRemoteExecutor(RemoteExecutor):
    def __init__(self, host, port=22, username="root", password=None, workdir="~/dflow/workflows/{{workflow.name}}/{{pod.name}}", command=None, remote_command=None,
            image="dptechnology/dflow-extender", header="", interval=3):
        super().__init__(host=host, port=port, username=username, password=password, workdir=workdir, command=command, remote_command=remote_command, image=image)
        self.header = header
        self.interval = interval

    def run(self):
        script = ""
        script += "echo '%s\n%s script' > slurm.sh\n" % (self.header, " ".join(self.remote_command))
        script += self.upload("slurm.sh", "%s/slurm.sh" % self.workdir) + " || exit 1\n"
        script += "echo 'jobIdFile: /tmp/job_id.txt' >> param.yaml\n"
        script += "echo 'workdir: %s' >> param.yaml\n" % self.workdir
        script += "echo 'scriptFile: slurm.sh' >> param.yaml\n"
        script += "echo 'interval: %s' >> param.yaml\n" % self.interval
        script += "echo 'host: %s' >> param.yaml\n" % self.host
        script += "echo 'port: %s' >> param.yaml\n" % self.port
        script += "echo 'username: %s' >> param.yaml\n" % self.username
        script += "echo 'password: %s' >> param.yaml\n" % self.password
        script += "./bin/slurm param.yaml || exit 1\n"
        return script
