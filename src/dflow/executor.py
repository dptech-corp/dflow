import abc
import os
import shlex
from abc import ABC
from copy import deepcopy
from typing import List, Optional, Union

from .common import S3Artifact
from .config import config
from .io import InputArtifact
from .op_template import OPTemplate, ScriptOPTemplate
from .utils import randstr, upload_s3

try:
    from argo.workflows.client import (V1HostPathVolumeSource, V1Volume,
                                       V1VolumeMount)
except Exception:
    pass


class Executor(ABC):
    """
    Executor
    """
    @abc.abstractmethod
    def render(
            self,
            template: OPTemplate,
    ) -> OPTemplate:
        """
        render original template and return a new template, do not modify
        self in this method to make the executor reusable
        """
        raise NotImplementedError()


def run_script(image, cmd, docker=None, singularity=None, podman=None,
               image_pull_policy=None, host_mounts=None, cpu=None,
               memory=None, args="", envs=None):
    if docker is not None:
        if image_pull_policy is None:
            if image.split(":")[-1] == "latest":
                image_pull_policy = "Always"
            else:
                image_pull_policy = "IfNotPresent"
        if host_mounts is not None:
            args += " " + " ".join(["-v%s:%s" % (v, k) for k, v in
                                    host_mounts.items()])
        if cpu is not None:
            args += " --cpus %s" % cpu
        if memory is not None:
            args += " --memory %s" % memory
        if envs is not None:
            args += " " + " ".join(["-e %s=%s" % (k, shlex.quote(v))
                                    for k, v in envs.items()])
        script = ""
        if image_pull_policy == "Always":
            script += "%s pull %s && " % (docker, image)
        elif image_pull_policy == "IfNotPresent":
            script += "if [ $(docker images %s | wc -l) -lt 2 ]; " % image
            script += "then %s pull %s; fi && " % (docker, image)
        return script + "%s run -v$(pwd)/tmp:/tmp "\
            "-v$(pwd)/script:/script %s %s %s /script" % (
                docker, args, image, " ".join(cmd))
    elif singularity is not None:
        if host_mounts is not None:
            args += " " + " ".join(["-B%s:%s" % (v, k) for k, v in
                                    host_mounts.items()])
        if envs is not None:
            args += " " + " ".join(["--env %s=%s" % (k, shlex.quote(v))
                                    for k, v in envs.items()])
        return "if [ -f %s ]; then rm -f image.sif && ln -s %s image.sif; "\
            "else %s pull image.sif %s; fi && %s run -B$(pwd)/tmp:/tmp "\
            "-B$(pwd)/script:/script %s image.sif %s /script && rm "\
            "image.sif" % (image, image, singularity, image, singularity,
                           args, " ".join(cmd))
    elif podman is not None:
        if host_mounts is not None:
            args += " " + " ".join(["-v%s:%s" % (v, k) for k, v in
                                    host_mounts.items()])
        if cpu is not None:
            args += " --cpus %s" % cpu
        if memory is not None:
            args += " --memory %s" % memory
        if envs is not None:
            args += " " + " ".join(["-e %s=%s" % (k, shlex.quote(v))
                                    for k, v in envs.items()])
        return "%s pull %s && %s run -v$(pwd)/tmp:/tmp "\
            "-v$(pwd)/script:/script %s %s %s /script" % (
                podman, image, podman, args, image, " ".join(cmd))
    else:
        return "%s script" % " ".join(cmd)


def render_script_with_tmp_root(template, tmp_root):
    if hasattr(template, "render_script"):
        tmp_template = deepcopy(template)
        tmp_template.tmp_root = tmp_root
        tmp_template.render_script()
        return tmp_template.script
    else:
        return template.script.replace("/tmp", tmp_root)


class ContainerExecutor(Executor):
    def __init__(
            self,
            docker: Optional[str] = None,
            singularity: Optional[str] = None,
            podman: Optional[str] = None,
            image_pull_policy: Optional[str] = None,
    ):
        self.docker = docker
        self.singularity = singularity
        self.podman = podman
        self.image_pull_policy = image_pull_policy

    def render(self, template):
        if not isinstance(template, ScriptOPTemplate):
            return template

        new_template = deepcopy(template)
        new_template.name += "-" + randstr()
        script = "cat <<'EOF'> script\n" + template.script + "\nEOF\n"
        prep_script = "import os, shutil\n"
        prep_script += "for dn, ds, fs in os.walk('tmp'):\n"
        prep_script += "    for d in ds:\n"
        prep_script += "        d = os.path.join(dn, d)\n"
        prep_script += "        if os.path.islink(d):\n"
        prep_script += "            src = os.path.realpath(d)\n"
        prep_script += "            os.remove(d)\n"
        prep_script += "            shutil.copytree(src, d, copy_function="\
            "os.link)\n"
        prep_script += "    for f in fs:\n"
        prep_script += "        f = os.path.join(dn, f)\n"
        prep_script += "        if os.path.islink(f):\n"
        prep_script += "            src = os.path.realpath(f)\n"
        prep_script += "            os.remove(f)\n"
        prep_script += "            os.link(src, f)\n"
        script += "cat <<'EOF' | python3\n" + prep_script + "\nEOF\n"
        host_mounts = {}
        for mount in template.mounts:
            name = getattr(mount, "name", mount["name"])
            volume = next(filter(lambda v: getattr(
                v, "name", v["name"]) == name, template.volumes))
            host_path = getattr(volume, "host_path", volume["hostPath"])
            host_path = getattr(host_path, "path", host_path["path"])
            mount_path = getattr(mount, "mount_path", mount["mountPath"])
            sub_path = getattr(mount, "sub_path", mount.get("subPath"))
            if sub_path:
                host_mounts[mount_path] = os.path.join(host_path, sub_path)
            else:
                host_mounts[mount_path] = host_path
        for art in template.inputs.artifacts.values():
            if not art.path.startswith("/tmp"):
                host_mounts[art.path] = "$(pwd)/%s" % art.path
        for art in template.outputs.artifacts.values():
            if not art.path.startswith("/tmp"):
                dir_path = os.path.dirname(art.path)
                assert dir_path != "/", "Output path in '/' is not allowed"
                host_mounts[dir_path] = "$(pwd)/%s" % dir_path
        for par in template.outputs.parameters.values():
            if not par.value_from_path.startswith("/tmp"):
                dir_path = os.path.dirname(par.value_from_path)
                assert dir_path != "/", "Output path in '/' is not allowed"
                host_mounts[dir_path] = "$(pwd)/%s" % dir_path
        cpu = None
        if template.requests is not None and "cpu" in template.requests:
            cpu = template.requests["cpu"]
            if isinstance(cpu, str) and cpu.endswith("m"):
                cpu = int(cpu[:-1]) / 1000
        memory = None
        if template.requests is not None and "memory" in template.requests:
            memory = template.requests["memory"]
            if isinstance(memory, str) and memory.endswith("Mi"):
                memory = memory[:-2] + "m"
            elif isinstance(memory, str) and memory.endswith("Gi"):
                memory = memory[:-2] + "g"
        script += run_script(template.image, template.command, self.docker,
                             self.singularity, self.podman,
                             self.image_pull_policy, host_mounts=host_mounts,
                             cpu=cpu, memory=memory, envs=template.envs)
        new_template.command = ["sh"]
        new_template.script = script
        new_template.script_rendered = True
        return new_template


class RemoteExecutor(Executor):
    def __init__(
            self,
            host: str,
            port: int = 22,
            username: str = "root",
            password: Optional[str] = None,
            private_key_file: Optional[os.PathLike] = None,
            workdir: str = "~/dflow/workflows/{{workflow.name}}/{{pod.name}}",
            command: Union[str, List[str]] = None,
            remote_command: Union[str, List[str]] = None,
            image: Optional[str] = None,
            image_pull_policy: Optional[str] = None,
            map_tmp_dir: bool = True,
            docker_executable: Optional[str] = None,
            singularity_executable: Optional[str] = None,
            podman_executable: Optional[str] = None,
            action_retries: int = -1,
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
        if image is None:
            image = config["extender_image"]
        self.image = image
        if image_pull_policy is None:
            image_pull_policy = config["extender_image_pull_policy"]
        self.image_pull_policy = image_pull_policy
        self.map_tmp_dir = map_tmp_dir
        self.docker_executable = docker_executable
        self.singularity_executable = singularity_executable
        self.podman_executable = podman_executable
        if self.docker_executable is not None or \
                self.singularity_executable is not None or \
                self.podman_executable is not None:
            self.map_tmp_dir = False
        self.action_retries = action_retries

    def execute(self, cmd):
        # add '' in case shell will expand ~
        return "execute %s '%s'" % (self.action_retries, cmd)

    def upload(self, src, dst):
        # add '' in case shell will expand ~
        return "upload %s '%s' '%s'" % (self.action_retries, src, dst)

    def download(self, src, dst):
        # add '' in case shell will expand ~
        return "download %s '%s' '%s'" % (self.action_retries, src, dst)

    def run(self, image, remote_command):
        script = "cd %s && " % self.workdir
        return self.execute(script + run_script(
            image, remote_command, self.docker_executable,
            self.singularity_executable, self.podman_executable)) + \
            " || exit 1\n"

    def upload_if_exists(self, path):
        return "if [ -e %s ]; then " % path \
            + self.upload(path, "%s/%s" % (
                self.workdir, os.path.dirname(path))) + "; fi\n"

    def mkdir_and_download(self, path):
        return "mkdir -p %s" % os.path.dirname(path) + "\n" + \
            self.download("%s/%s" % (self.workdir, path),
                          os.path.dirname(path)) + "\n"

    def get_script(self, template):
        if self.map_tmp_dir:
            remote_script = render_script_with_tmp_root(
                template, "%s/tmp" % self.workdir)
        else:
            remote_script = template.script
        remote_command = template.command if self.remote_command is None \
            else self.remote_command
        ssh_pass = "sshpass -p %s " % self.password if self.password is not \
            None else ""
        identity = "-i /root/.ssh/%s " % os.path.basename(
            self.private_key_file) if self.private_key_file is not None else ""
        script = """
execute() {
    if [ $1 != 0 ]; then
        %sssh %s-C -o StrictHostKeyChecking=no -p %s %s@%s -- $2
        if [ $? != 0 ]; then
            echo retry: $1
            sleep 1
            execute $(($1-1)) '$2'
        fi
    fi
}
""" % (ssh_pass, identity, self.port, self.username, self.host)
        script += """
upload() {
    if [ $1 != 0 ]; then
        %sssh %s-C -o StrictHostKeyChecking=no -p %s %s@%s -- mkdir -p $3 && \
        %sscp %s-C -o StrictHostKeyChecking=no -P %s -r $2 %s@%s:$3
        if [ $? != 0 ]; then
            echo retry: $1
            sleep 1
            upload $(($1-1)) $2 $3
        fi
    fi
}
""" % (ssh_pass, identity, self.port, self.username, self.host,
            ssh_pass, identity, self.port, self.username, self.host)
        script += """
download() {
    if [ $1 != 0 ]; then
        %sscp %s-C -o StrictHostKeyChecking=no -P %s -r %s@%s:$2 $3
        if [ $? != 0 ]; then
            echo retry: $1
            sleep 1
            download $(($1-1)) $2 $3
        fi
    fi
}
""" % (ssh_pass, identity, self.port, self.username, self.host)
        script += "cat <<'EOF'> script\n" + remote_script + "\nEOF\n"
        for art in template.inputs.artifacts.values():
            script += self.upload_if_exists(art.path)
        for par in template.inputs.parameters.values():
            if par.save_as_artifact:
                script += self.upload_if_exists(par.path)
        script += self.upload_if_exists("script")
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
        new_template.name += "-" + randstr()
        new_template.image = self.image
        new_template.image_pull_policy = self.image_pull_policy
        new_template.command = self.command
        new_template.script = self.get_script(template)
        if self.password is not None:
            pass
        elif self.private_key_file is not None:
            key = upload_s3(self.private_key_file)
            private_key_artifact = S3Artifact(key=key)
            new_template.inputs.artifacts["dflow_private_key"] = InputArtifact(
                path="/root/.ssh/" + os.path.basename(self.private_key_file),
                source=private_key_artifact, mode=0o600)
        elif config["private_key_host_path"] is not None:
            new_template.volumes.append(V1Volume(
                name="dflow-private-key", host_path=V1HostPathVolumeSource(
                    path=config["private_key_host_path"])))
            new_template.mounts.append(V1VolumeMount(
                name="dflow-private-key", mount_path="/root/.ssh"))
        new_template.script_rendered = True
        return new_template
