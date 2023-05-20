import json
import logging
import os
import sys
import time
from copy import deepcopy
from typing import Any, Dict, List, Optional, Union

import jsonpickle

from .argo_objects import ArgoStep, ArgoWorkflow
from .config import config, s3_config
from .context import Context
from .context_syntax import GLOBAL_CONTEXT
from .dag import DAG
from .executor import Executor
from .op_template import (ContainerOPTemplate, OPTemplate, ScriptOPTemplate,
                          get_k8s_client)
from .step import Step
from .steps import Steps
from .task import Task
from .utils import copy_s3, get_key, linktree, randstr, set_key

try:
    import urllib3

    import kubernetes
    urllib3.disable_warnings()
    import yaml
    from argo.workflows.client import (ApiClient, Configuration,
                                       V1alpha1Arguments,
                                       V1alpha1ArtifactRepositoryRef,
                                       V1alpha1Parameter, V1alpha1PodGC,
                                       V1alpha1Workflow,
                                       V1alpha1WorkflowCreateRequest,
                                       V1alpha1WorkflowSpec,
                                       V1LocalObjectReference, V1ObjectMeta,
                                       V1PersistentVolumeClaim,
                                       V1PersistentVolumeClaimSpec,
                                       V1ResourceRequirements,
                                       WorkflowServiceApi)
except Exception:
    pass

logger = logging.getLogger(__name__)


class DockerSecret:
    def __init__(self, registry, username, password, name=None):
        self.registry = registry
        self.username = username
        self.password = password
        if name is None:
            name = "dflow-%s" % randstr()
        self.name = name


class Workflow:
    """
    Workflow

    Args:
        name: the name of the workflow
        steps: steps used as the entrypoint of the workflow, if not provided,
            a empty steps will be used
        dag: dag used as the entrypoint of the workflow
        namespace: k8s namespace
        id: workflow ID in Argo, you can provide it to track an existing
            workflow
        host: URL of the Argo server, will override global config
        token: request the Argo server with the token, will override global
            config
        k8s_config_file: Kubernetes configuration file for accessing API
            server, will override global config
        k8s_api_server: Url of kubernetes API server, will override global
            config
        context: context for the workflow
        annotations: annotations for the workflow
        parallelism: maximum number of running pods for the workflow
        pod_gc_stategy: pod GC provides the ability to delete pods
            automatically without deleting the workflow, pod GC strategy
            must be one of the following:

            * OnPodCompletion - delete pods immediately when pod is completed
                (including errors/failures)
            * OnPodSuccess - delete pods immediately when pod is successful
            * OnWorkflowCompletion - delete pods when workflow is completed
            * OnWorkflowSuccess - delete pods when workflow is successful
        image_pull_secrets: secrets for image registies
        artifact_repo_key: use artifact repository reference by key
        parameters: global input parameters
    """

    def __init__(
            self,
            name: str = "workflow",
            steps: Optional[Steps] = None,
            dag: Optional[DAG] = None,
            namespace: Optional[str] = None,
            id: Optional[str] = None,
            uid: Optional[str] = None,
            host: Optional[str] = None,
            token: Optional[str] = None,
            k8s_config_file: Optional[os.PathLike] = None,
            k8s_api_server: Optional[str] = None,
            context: Optional[Union[Context, Executor]] = None,
            annotations: Dict[str, str] = None,
            labels: Dict[str, str] = None,
            parallelism: Optional[int] = None,
            pod_gc_strategy: Optional[str] = None,
            image_pull_secrets: Optional[Union[
                str, DockerSecret, List[Union[str, DockerSecret]]]] = None,
            artifact_repo_key: Optional[str] = None,
            parameters: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.host = host if host is not None else config["host"]
        self.token = token if token is not None else config["token"]
        self.k8s_config_file = k8s_config_file if k8s_config_file is not None \
            else config["k8s_config_file"]
        self.k8s_api_server = k8s_api_server if k8s_api_server is not None \
            else config["k8s_api_server"]
        self.context = context
        if annotations is None:
            annotations = deepcopy(config["workflow_annotations"])
        self.annotations = annotations
        self.labels = labels
        self.parallelism = parallelism
        self.pod_gc_strategy = pod_gc_strategy
        if image_pull_secrets is not None and not isinstance(
                image_pull_secrets, list):
            image_pull_secrets = [image_pull_secrets]
        self.image_pull_secrets = image_pull_secrets
        self.artifact_repo_key = artifact_repo_key if artifact_repo_key is \
            not None else s3_config["repo_key"]

        api_client = get_argo_api_client(self.host, self.token)
        self.api_instance = WorkflowServiceApi(api_client)

        self.namespace = namespace if namespace is not None else \
            config["namespace"]
        self.id = id
        # alias uid to id if uid not provided
        if uid is None:
            uid = id
        self.uid = uid
        self.name = name
        if steps is not None:
            assert isinstance(steps, Steps)
            self.entrypoint = steps
        elif dag is not None:
            assert isinstance(dag, DAG)
            self.entrypoint = dag
        else:
            self.entrypoint = Steps(self.name + "-steps")
        self.templates = {}
        self.argo_templates = {}
        self.pvcs = {}
        if parameters is None:
            parameters = {}
        self.parameters = parameters
        self.k8s_client = None

        if self.artifact_repo_key is not None:
            core_v1_api = self.get_k8s_core_v1_api()
            cm = core_v1_api.read_namespaced_config_map(
                namespace=self.namespace, name="artifact-repositories")
            repo = yaml.full_load(cm.data[self.artifact_repo_key])
            s3_config["repo"] = repo
            if "s3" in repo:
                s3_config["repo_type"] = "s3"
                s3 = repo["s3"]
            elif "oss" in repo:
                s3_config["repo_type"] = "oss"
                s3 = repo["oss"]
            if "keyFormat" in s3:
                t = "{{workflow.name}}/{{pod.name}}"
                if s3["keyFormat"].endswith(t):
                    s3_config["repo_prefix"] = s3["keyFormat"][:-len(t)]

    def get_k8s_core_v1_api(self):
        if self.k8s_client is None:
            self.k8s_client = get_k8s_client(self.k8s_api_server, self.token,
                                             self.k8s_config_file)
        return kubernetes.client.CoreV1Api(self.k8s_client)

    def __enter__(self) -> 'Workflow':
        GLOBAL_CONTEXT.in_context = True
        GLOBAL_CONTEXT.current_workflow = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        GLOBAL_CONTEXT.in_context = False

    def add(
            self,
            step: Union[Step, List[Step], Task, List[Task]],
    ) -> None:
        """
        Add a step or a list of parallel steps to the workflow

        Args:
            step: a step or a list of parallel steps to be added to the
            entrypoint of the workflow
        """
        self.entrypoint.add(step)

    def submit(
            self,
            reuse_step: Optional[List[ArgoStep]] = None,
    ) -> ArgoWorkflow:
        """
        Submit the workflow

        Args:
            reuse_step: a list of steps to be reused in the workflow
        """
        if config["mode"] == "debug":
            if self.context is not None:
                assert isinstance(self.context, (Context, Executor))
                self = self.context.render(self)

            if self.id is None:
                while True:
                    self.id = self.name + "-" + randstr()
                    wfdir = os.path.abspath(self.id)
                    if not os.path.exists(wfdir):
                        os.makedirs(wfdir)
                        break
            else:
                wfdir = os.path.abspath(self.id)
                os.makedirs(wfdir, exist_ok=True)

            if reuse_step is not None:
                for step in reuse_step:
                    if step.key is None:
                        continue
                    stepdir = os.path.join(wfdir, step.key)
                    os.makedirs(stepdir, exist_ok=True)
                    with open(os.path.join(stepdir, "phase"), "w") as f:
                        f.write(step.phase)
                    for io in ["inputs", "outputs"]:
                        os.makedirs(os.path.join(stepdir, io, "parameters"),
                                    exist_ok=True)
                        for name, par in step[io].parameters.items():
                            with open(os.path.join(stepdir, io, "parameters",
                                                   name), "w") as f:
                                value = par.recover()["value"]
                                if isinstance(value, str):
                                    f.write(value)
                                else:
                                    f.write(jsonpickle.dumps(value))
                            if par.type is not None:
                                os.makedirs(os.path.join(
                                    stepdir, io, "parameters/.dflow"),
                                    exist_ok=True)
                                with open(os.path.join(
                                        stepdir, io, "parameters/.dflow",
                                        name), "w") as f:
                                    f.write(par.type)

                        os.makedirs(os.path.join(stepdir, io, "artifacts"),
                                    exist_ok=True)
                        if "dflow_group_key" in step.inputs.parameters:
                            key = step.inputs.parameters[
                                "dflow_group_key"].value
                            if not os.path.exists(os.path.join(wfdir, key)):
                                os.symlink(
                                    os.path.join(
                                        os.path.abspath(step.workflow), key),
                                    os.path.join(wfdir, key))
                        for name, art in step[io].artifacts.items():
                            if "dflow_group_key" in step.inputs.parameters:
                                key = step.inputs.parameters[
                                    "dflow_group_key"].value
                                if os.path.exists(os.path.join(wfdir, key,
                                                               name)):
                                    if not os.path.samefile(
                                            art.local_path,
                                            os.path.join(wfdir, key, name)):
                                        linktree(
                                            art.local_path,
                                            os.path.join(wfdir, key, name))
                                        os.symlink(
                                            os.path.join(wfdir, key, name),
                                            os.path.join(stepdir, io,
                                                         "artifacts", name))
                                else:
                                    os.symlink(art.local_path, os.path.join(
                                        stepdir, io, "artifacts", name))
                            else:
                                os.symlink(art.local_path, os.path.join(
                                    stepdir, io, "artifacts", name))

            cwd = os.getcwd()
            os.chdir(wfdir)
            print("Workflow is running locally (ID: %s)" % self.id)
            with open(os.path.join(wfdir, "status"), "w") as f:
                f.write("Running")
            if config["detach"]:
                pid = os.fork()
                if pid != 0:
                    os.chdir(cwd)
                    return ArgoWorkflow({"id": self.id})
                flog = open(os.path.join(wfdir, "log.txt"), "w")
                os.dup2(flog.fileno(), sys.stdout.fileno())
                os.dup2(flog.fileno(), sys.stderr.fileno())
            try:
                entrypoint = deepcopy(self.entrypoint)
                entrypoint.orig_template = self.entrypoint
                entrypoint.run(self.id, self.context)
                with open(os.path.join(wfdir, "status"), "w") as f:
                    f.write("Succeeded")
            except Exception:
                import traceback
                traceback.print_exc()
                with open(os.path.join(wfdir, "status"), "w") as f:
                    f.write("Failed")
            if config["detach"]:
                flog.close()
                exit()
            os.chdir(cwd)
            return ArgoWorkflow({"id": self.id})

        manifest = self.convert_to_argo(reuse_step=reuse_step)

        logger.debug("submit manifest:\n%s" % manifest)
        response = self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s' % self.namespace, 'POST',
            body=V1alpha1WorkflowCreateRequest(workflow=manifest),
            response_type=object,
            header_params=config["http_headers"],
            _return_http_data_only=True)
        workflow = ArgoWorkflow(response)

        self.id = workflow.metadata.name
        self.uid = workflow.metadata.uid
        print("Workflow has been submitted (ID: %s, UID: %s)" % (self.id,
                                                                 self.uid))
        return workflow

    def wait(self, interval=1):
        while self.query_status() in ["Pending", "Running"]:
            time.sleep(interval)

    def handle_reused_step(self, step):
        outputs = {}
        if hasattr(step, "outputs"):
            if hasattr(step.outputs, "exitCode"):
                outputs["exitCode"] = step.outputs.exitCode
            if hasattr(step.outputs, "parameters"):
                outputs["parameters"] = []
                for name, par in step.outputs.parameters.items():
                    if not hasattr(step.outputs.parameters[name],
                                   "save_as_artifact"):
                        outputs["parameters"].append(par.recover())
            if hasattr(step.outputs, "artifacts"):
                for name, art in step.outputs.artifacts.items():
                    if hasattr(step, "inputs") and \
                        hasattr(step.inputs, "parameters") and \
                        "dflow_group_key" in step.inputs.parameters \
                            and name != "main-logs":
                        if config["overwrite_reused_artifact"]:
                            self.handle_reused_artifact(step, name, art)
                        else:
                            self.handle_reused_artifact_with_copy(
                                step, name, art)

        outputs["artifacts"] = [
            art.recover() for art in step.outputs.artifacts.values()]
        self.memoize_map["%s-%s" % (self.id, step.key)] = {
            "nodeID": step.id,
            "outputs": outputs,
            "creationTimestamp": step.finishedAt,
            "lastHitTimestamp": step.finishedAt
        }

    def handle_reused_artifact(self, step, name, art):
        art_key = get_key(art, raise_error=False)
        if hasattr(art, "modified"):
            key = art.modified["old_key"]
            logger.debug("copying artifact: %s -> %s" % (art_key, key))
            copy_s3(art_key, key)
            set_key(art, key)
            art_key = key

        art_key_prefix = art_key
        if art_key_prefix.startswith(s3_config["prefix"]):
            art_key_prefix = art_key_prefix[len(s3_config["prefix"]):]
        if art_key_prefix.endswith(name):
            art_key_prefix = art_key_prefix[:-len(name)-1]

        group_key = step.inputs.parameters["dflow_group_key"].value
        if "%s-init-artifact" % group_key in self.reused_keys:
            return
        memoize_key = "%s-%s-init-artifact" % (self.id, group_key)
        if memoize_key not in self.memoize_map:
            self.memoize_map[memoize_key] = {
                "nodeID": "dflow-%s" % randstr(10),
                "outputs": {
                    "parameters": [{
                        "name": "dflow_artifact_key",
                        "value": art_key_prefix,
                    }],
                    "artifacts": [],
                },
                "creationTimestamp": step.finishedAt,
                "lastHitTimestamp": step.finishedAt,
            }
        init_step = self.memoize_map[memoize_key]

        init_art = None
        for art in init_step["outputs"]["artifacts"]:
            if art["name"] == name:
                init_art = art
                break
        if not init_art:
            init_art = {"name": name, "archive": {"none": {}}}
            if s3_config["repo_type"] == "s3":
                init_art["s3"] = {"key": art_key}
            elif s3_config["repo_type"] == "oss":
                init_art["oss"] = {"key": s3_config["repo_prefix"] + art_key}
            init_step["outputs"]["artifacts"].append(init_art)

    def handle_reused_artifact_with_copy(self, step, name, art):
        old_key = get_key(art, raise_error=False)
        if old_key and old_key not in self.copied_keys:
            key = "%s%s/%s/%s" % (
                s3_config["prefix"], self.id, step.inputs.parameters[
                    "dflow_group_key"].value, name)
            logger.debug("copying artifact: %s -> %s" % (old_key, key))
            copy_s3(old_key, key)
            set_key(art, key)
            self.copied_keys.append(old_key)

    def convert_to_argo(self, reuse_step=None):
        if self.context is not None:
            assert isinstance(self.context, (Context, Executor))
            self = self.context.render(self)

        status = None
        if reuse_step is not None:
            self.reused_keys = [step.key for step in reuse_step
                                if step.key is not None]
            if self.id is None:
                self.id = self.name + "-" + randstr()
            self.copied_keys = []
            self.memoize_map = {}
            key2id = {}
            for step in reuse_step:
                data = {}
                if step.key is None:
                    continue
                key2id[step.key] = step.id
                self.handle_reused_step(step)

            for key, step in self.memoize_map.items():
                data = {key: json.dumps(step)}
                config_map = kubernetes.client.V1ConfigMap(
                    data=data, metadata=kubernetes.client.V1ObjectMeta(
                        name="dflow-%s" % key))
                core_v1_api = self.get_k8s_core_v1_api()
                logger.debug("creating configmap: %s" %
                             config_map.metadata.name)
                core_v1_api.api_client.call_api(
                    '/api/v1/namespaces/%s/configmaps' % self.namespace,
                    'POST', body=config_map, response_type='V1ConfigMap',
                    header_params=config["http_headers"],
                    _return_http_data_only=True)

            self.handle_template(self.entrypoint, memoize_prefix=self.id,
                                 memoize_configmap="dflow")
            status = {"outputs": {"parameters": [
                {"name": key, "value": id} for key, id in key2id.items()]}}
        else:
            self.handle_template(self.entrypoint)

        argo_pvcs = []
        for pvc in self.pvcs.values():
            argo_pvcs.append(V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name=pvc.name),
                spec=V1PersistentVolumeClaimSpec(
                    storage_class_name=pvc.storage_class,
                    access_modes=pvc.access_modes,
                    resources=V1ResourceRequirements(
                        requests={"storage": pvc.size}
                    )
                )
            ))

        if self.id is not None:
            metadata = V1ObjectMeta(
                name=self.id, annotations=self.annotations, labels=self.labels)
        else:
            metadata = V1ObjectMeta(
                generate_name=self.name + '-', annotations=self.annotations,
                labels=self.labels)

        if self.image_pull_secrets is not None:
            for i, s in enumerate(self.image_pull_secrets):
                if isinstance(s, DockerSecret):
                    data = {".dockerconfigjson": json.dumps({
                        "auths": {s.registry: {
                            "username": s.username, "password": s.password}}})}
                    secret = kubernetes.client.V1Secret(
                        string_data=data,
                        metadata=kubernetes.client.V1ObjectMeta(name=s.name),
                        type="kubernetes.io/dockerconfigjson")
                    core_v1_api = self.get_k8s_core_v1_api()
                    core_v1_api.api_client.call_api(
                        '/api/v1/namespaces/%s/secrets' % self.namespace,
                        'POST', body=secret, response_type='V1Secret',
                        header_params=config["http_headers"],
                        _return_http_data_only=True)
                    self.image_pull_secrets[i] = s.name

        if config["register_tasks"]:
            workflow_urn = config["lineage"].register_workflow(self.name)
            self.parameters["dflow_workflow_urn"] = workflow_urn

        return V1alpha1Workflow(
            metadata=metadata,
            spec=V1alpha1WorkflowSpec(
                arguments=V1alpha1Arguments(
                    parameters=[V1alpha1Parameter(
                        name=k, value=v if isinstance(v, str) else
                        jsonpickle.dumps(v)) for k, v in
                        self.parameters.items()]),
                service_account_name='argo',
                entrypoint=self.entrypoint.name,
                templates=list(self.argo_templates.values()),
                parallelism=self.parallelism,
                volume_claim_templates=argo_pvcs,
                pod_gc=V1alpha1PodGC(strategy=self.pod_gc_strategy),
                image_pull_secrets=None if self.image_pull_secrets is None else
                [V1LocalObjectReference(s) for s in self.image_pull_secrets],
                artifact_repository_ref=None if self.artifact_repo_key is None
                else V1alpha1ArtifactRepositoryRef(key=self.artifact_repo_key)
            ),
            status=status)

    def to_dict(self):
        return self.api_instance.api_client.sanitize_for_serialization(
            self.convert_to_argo())

    def to_json(self, **kwargs):
        return json.dumps(self.to_dict(), **kwargs)

    def to_yaml(self, **kwargs):
        return yaml.dump(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, s):
        return cls.from_dict(json.loads(s))

    @classmethod
    def from_yaml(cls, s):
        return cls.from_dict(yaml.full_load(s))

    @classmethod
    def from_dict(cls, d):
        kwargs = {
            "name": d.get("metadata", {}).get(
                "generateName", "workflow").strip("-"),
            "namespace": d.get("metadata", {}).get("namespace", None),
            "id": d.get("metadata", {}).get("name", None),
            "annotations": d.get("metadata", {}).get("annotations", None),
            "labels": d.get("metadata", {}).get("labels", None),
            "parallelism": d.get("spec", {}).get("parallelism", None),
            "pod_gc_strategy": d.get("spec", {}).get("podGC", {}).get(
                "strategy", None),
            "artifact_repo_key": d.get("spec", {}).get(
                "artifact_repository_ref", {}).get("key", None),
            "image_pull_secrets": d.get("spec", {}).get("imagePullSecrets",
                                                        None),
            "parameters": {par["name"]: par["value"] for par in d.get(
                "spec", {}).get("arguments", {}).get("parameters", [])}
        }
        templates = {}
        for template in d["spec"]["templates"]:
            name = template["name"]
            if "script" in template:
                templates[name] = ScriptOPTemplate()
            elif "steps" in template:
                templates[name] = Steps()
            elif "dag" in template:
                templates[name] = DAG()
            elif "container" in template:
                templates[name] = ContainerOPTemplate()
            templates[name].__dict__.update(
                OPTemplate.from_dict(template).__dict__)
        for template in d["spec"]["templates"]:
            name = template["name"]
            if "script" in template:
                templates[name].__dict__.update(
                    ScriptOPTemplate.from_dict(template).__dict__)
            elif "steps" in template:
                templates[name].__dict__.update(
                    Steps.from_dict(template, templates).__dict__)
            elif "dag" in template:
                templates[name].__dict__.update(
                    DAG.from_dict(template, templates).__dict__)
            elif "container" in template:
                templates[name].__dict__.update(
                    ContainerOPTemplate.from_dict(template).__dict__)
        entrypoint = templates[d["spec"]["entrypoint"]]
        if isinstance(entrypoint, ScriptOPTemplate):
            kwargs["steps"] = Steps()
            step = Step("main", entrypoint,
                        parameters=kwargs.pop("parameters"))
            kwargs["steps"].add(step)
        elif isinstance(entrypoint, Steps):
            kwargs["steps"] = entrypoint
        elif isinstance(entrypoint, DAG):
            kwargs["dag"] = entrypoint
        engine = d.get("metadata", {}).get("annotations", {}).get(
            "workflow.dp.tech/container_engine")
        docker = "docker" if engine == "docker" else None
        singularity = "singularity" if engine == "singularity" else None
        podman = "podman" if engine == "podman" else None
        if d.get("metadata", {}).get("annotations", {}).get(
                "workflow.dp.tech/executor") == "dispatcher":
            host = kwargs["annotations"].get("workflow.dp.tech/host")
            port = int(kwargs["annotations"].get("workflow.dp.tech/port", 22))
            username = kwargs["annotations"].get(
                "workflow.dp.tech/username", "root")
            password = kwargs["annotations"].get("workflow.dp.tech/password")
            queue_name = kwargs["annotations"].get(
                "workflow.dp.tech/queue_name")
            extras = kwargs["annotations"].get("workflow.dp.tech/extras")
            extras = json.loads(extras) if extras else {}
            machine = extras.get("machine", None)
            resources = extras.get("resources", None)
            task = extras.get("task", None)
            from .plugins.dispatcher import DispatcherExecutor
            kwargs["context"] = DispatcherExecutor(
                host, queue_name, port, username, password,
                machine_dict=machine, resources_dict=resources,
                task_dict=task, docker_executable=docker,
                singularity_executable=singularity, podman_executable=podman)
        elif engine:
            from .executor import ContainerExecutor
            kwargs["context"] = ContainerExecutor(docker, singularity, podman)
        return cls(**kwargs)

    def handle_template(self, template, memoize_prefix=None,
                        memoize_configmap="dflow"):
        if template.name in self.templates:
            assert template == self.templates[template.name], \
                "Duplication of template name: %s" % template.name
        else:
            logger.debug("handle template %s" % template.name)
            self.templates[template.name] = template
            # if the template is steps or dag, handle involved templates
            if isinstance(template, (Steps, DAG)):
                # breadth first algorithm
                argo_template, templates = template.convert_to_argo(
                    memoize_prefix, memoize_configmap, self.context)
                self.argo_templates[template.name] = argo_template
                for template in templates:
                    self.handle_template(
                        template, memoize_prefix, memoize_configmap)
            else:
                self.argo_templates[template.name] = template.convert_to_argo(
                    memoize_prefix, memoize_configmap)
                for pvc in template.pvcs:
                    if pvc.name not in self.pvcs:
                        self.pvcs[pvc.name] = pvc

    def query(
            self,
            fields: Optional[List[str]] = None,
    ) -> ArgoWorkflow:
        """
        Query the workflow from Argo
        If fields is not provided, full information of all steps will be
        returned [O(n)]

        Args:
            fields: fields of the workflow to be returned
        Returns:
            an ArgoWorkflow object
        """
        query_params = None
        if fields is not None:
            query_params = [('fields', ",".join(fields))]
        try:
            response = self.api_instance.api_client.call_api(
                '/api/v1/workflows/%s/%s' % (self.namespace, self.id),
                'GET', response_type=object, _return_http_data_only=True,
                header_params=config["http_headers"],
                query_params=query_params)
        except Exception:
            response = self.api_instance.api_client.call_api(
                '/api/v1/archived-workflows/%s' % self.uid,
                'GET', response_type=object, _return_http_data_only=True,
                header_params=config["http_headers"],
                query_params=query_params)
        workflow = ArgoWorkflow(response)
        return workflow

    def query_status(
            self,
    ) -> str:
        """
        Query the status of the workflow from Argo
        The function is O(1)

        Returns:
            Pending, Running, Succeeded, Failed, Error, etc
        """
        if config["mode"] == "debug":
            with open("%s/status" % self.id, "r") as f:
                return f.read()
        workflow = self.query(fields=["status.phase"])

        if "phase" not in workflow.status:
            return "Pending"
        else:
            return workflow.status.phase

    def query_step(
            self,
            name: Union[str, List[str]] = None,
            key: Union[str, List[str]] = None,
            phase: Union[str, List[str]] = None,
            id: Union[str, List[str]] = None,
            type: Union[str, List[str]] = None,
    ) -> List[ArgoStep]:
        """
        Query the existing steps of the workflow from Argo
        This function will query full steps from server [O(n)], then filter
        with conditions given in the arguments
        If you want to call this function multiple times successively,
        it is recommended to call query once and call get_step repeatedly, e.g.
        info = wf.query()
        step1 = info.get_step(key="step1")
        step2 = info.get_step(key="step2")

        Args:
            name: filter by name of step, support regex
            key: filter by key of step
            phase: filter by phase of step
            id: filter by id of step
            type: filter by type of step
        Returns:
            a list of steps
        """
        if config["mode"] == "debug":
            if key is not None and not isinstance(key, list):
                key = [key]
            step_list = []
            for s in os.listdir(self.id):
                stepdir = os.path.join(self.id, s)
                if not os.path.isdir(stepdir):
                    continue
                if name is not None and not s.startswith(self.id + "-" + name):
                    continue
                if key is not None and s not in key:
                    continue
                if not os.path.exists(os.path.join(stepdir, "type")):
                    continue
                with open(os.path.join(stepdir, "type"), "r") as f:
                    _type = f.read()
                if type is not None and type != _type:
                    continue
                if os.path.exists(os.path.join(stepdir, "phase")):
                    with open(os.path.join(stepdir, "phase"), "r") as f:
                        _phase = f.read()
                else:
                    _phase = "Pending"
                if phase is not None and phase != _phase:
                    continue
                step = {
                    "workflow": self.id,
                    "displayName": s,
                    "key": s,
                    "startedAt": os.path.getmtime(stepdir),
                    "phase": _phase,
                    "type": _type,
                    "inputs": {
                        "parameters": [],
                        "artifacts": [],
                    },
                    "outputs": {
                        "parameters": [],
                        "artifacts": [],
                    },
                }
                for io in ["inputs", "outputs"]:
                    if os.path.exists(os.path.join(stepdir, io, "parameters")):
                        for p in os.listdir(os.path.join(stepdir, io,
                                                         "parameters")):
                            if p == ".dflow":
                                continue
                            with open(os.path.join(stepdir, io, "parameters",
                                                   p), "r") as f:
                                val = f.read()
                            _type = None
                            if os.path.exists(os.path.join(
                                    stepdir, io, "parameters/.dflow", p)):
                                with open(os.path.join(
                                        stepdir, io, "parameters/.dflow", p),
                                        "r") as f:
                                    _type = json.load(f)["type"]
                                if _type != str(str):
                                    val = jsonpickle.loads(val)
                            step[io]["parameters"].append({
                                "name": p, "value": val, "type": _type})
                    if os.path.exists(os.path.join(stepdir, io, "artifacts")):
                        for a in os.listdir(os.path.join(stepdir, io,
                                                         "artifacts")):
                            step[io]["artifacts"].append({
                                "name": a,
                                "local_path": os.path.abspath(os.path.join(
                                    stepdir, io, "artifacts", a)),
                            })
                step = ArgoStep(step, self.id)
                step_list.append(step)
            return step_list

        return self.query().get_step(name=name, key=key, phase=phase, id=id,
                                     type=type)

    def query_keys_of_steps(
            self,
    ) -> List[str]:
        """
        Query the keys of existing steps of the workflow from Argo
        This function will try to get keys from the global outputs,
        which is O(1). If failed, it will downgrade to query full steps

        Returns:
            a list of keys
        """
        if config["mode"] == "debug":
            return [step.key for step in self.query_step()]
        try:
            try:
                response = self.api_instance.api_client.call_api(
                    '/api/v1/workflows/%s/%s' % (self.namespace, self.id),
                    'GET', response_type=object, _return_http_data_only=True,
                    header_params=config["http_headers"],
                    query_params=[('fields', 'status.outputs')])
            except Exception:
                response = self.api_instance.api_client.call_api(
                    '/api/v1/archived-workflows/%s' % self.uid,
                    'GET', response_type=object, _return_http_data_only=True,
                    header_params=config["http_headers"],
                    query_params=[('fields', 'status.outputs')])
            return [par["name"] for par in
                    response["status"]["outputs"]["parameters"]]
        except Exception:
            logger.warn("Key-ID map not found in the global outputs, downgrade"
                        " to full query")
            return [step.key for step in self.query_step()
                    if step.key is not None]

    def query_step_by_key(
            self,
            key: Union[str, List[str]],
            name: Union[str, List[str]] = None,
            phase: Union[str, List[str]] = None,
            id: Union[str, List[str]] = None,
            type: Union[str, List[str]] = None,
    ) -> List[ArgoStep]:
        """
        Query the existing steps of the workflow from Argo by key
        This function will try to get key-ID map from the global outputs,
        then query step by ID, which is O(m) where m is the number of the
        requested keys. If failed, it will downgrade to query full steps

        Args:
            key: filter by key of step
        Returns:
            a list of steps
        """
        if isinstance(key, str):
            key = [key]

        try:
            workflow = self.query(fields=['metadata.name', 'status.outputs'])
            wf_name = workflow.metadata.name
            key2id = {}
            for par in workflow.status.outputs.parameters:
                pod_name = par["value"]
                key2id[par["name"]] = wf_name + "-" + pod_name.split("-")[-1]

            workflow = self.query(
                fields=['metadata.name'] + [
                    'status.nodes.' + key2id[k] for k in key])
            return workflow.get_step(name=name, phase=phase, id=id, type=type)
        except Exception:
            logger.warn("Key-ID map not found in the global outputs, downgrade"
                        " to full query")
            return self.query_step(key=key, name=name, phase=phase, id=id,
                                   type=type)

    def query_global_outputs(self) -> ArgoWorkflow:
        """
        Query the global outputs of the workflow from Argo
        The function is O(1)

        Args:
            key: filter by key of step
        Returns:
            a list of steps
        """
        workflow = self.query(fields=['metadata.name', 'status.outputs'])
        step = ArgoStep(workflow.status, workflow.metadata.name)
        if hasattr(step, "outputs"):
            return step.outputs
        else:
            return None

    def terminate(self) -> None:
        """
        Terminate the workflow
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s/%s/terminate' % (self.namespace, self.id),
            'PUT', header_params=config["http_headers"])

    def delete(self) -> None:
        """
        Delete the workflow
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s/%s' % (self.namespace, self.id), 'DELETE',
            header_params=config["http_headers"])

    def resubmit(self) -> None:
        """
        Resubmit the workflow
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s/%s/resubmit' % (self.namespace, self.id),
            'PUT', header_params=config["http_headers"])

    def resume(self) -> None:
        """
        Resume the workflow
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s/%s/resume' % (self.namespace, self.id),
            'PUT', header_params=config["http_headers"])

    def retry(self) -> None:
        """
        Retry the workflow
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s/%s/retry' % (self.namespace, self.id),
            'PUT', header_params=config["http_headers"])

    def stop(self) -> None:
        """
        Stop the workflow
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s/%s/stop' % (self.namespace, self.id),
            'PUT', header_params=config["http_headers"])

    def suspend(self) -> None:
        """
        Suspend the workflow
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s/%s/suspend' % (self.namespace, self.id),
            'PUT', header_params=config["http_headers"])


def get_argo_api_client(host=None, token=None):
    if host is None:
        host = config["host"]
    if token is None:
        token = config["token"]
    configuration = Configuration(host=host)
    configuration.verify_ssl = False
    if token is None:
        api_client = ApiClient(configuration)
    else:
        api_client = ApiClient(
            configuration, header_name='Authorization',
            header_value='Bearer %s' % token)
    return api_client


def query_workflows(labels: Optional[Dict[str, str]] = None,
                    fields: Optional[List[str]] = None) -> List[ArgoWorkflow]:
    if fields is None:
        fields = [
            'metadata', 'items.metadata.uid', 'items.metadata.name',
            'items.metadata.namespace', 'items.metadata.creationTimestamp',
            'items.metadata.labels', 'items.metadata.annotations',
            'items.status.phase', 'items.status.message',
            'items.status.finishedAt', 'items.status.startedAt',
            'items.status.estimatedDuration', 'items.status.progress',
            'items.spec.suspend',
        ]
    query_params = [('fields', ",".join(fields))]
    if labels is not None:
        query_params.append((
            'listOptions.labelSelector',
            ",".join(["%s=%s" % (k, v) for k, v in labels.items()])))
    with get_argo_api_client() as api_client:
        res = api_client.call_api(
            '/api/v1/workflows/%s' % config["namespace"],
            'GET', response_type='object',
            header_params=config["http_headers"],
            query_params=query_params,
            _return_http_data_only=True)
    return [ArgoWorkflow(w) for w in res["items"]] if res["items"] else []


def query_archived_workflows(
        labels: Optional[Dict[str, str]] = None) -> List[ArgoWorkflow]:
    query_params = [('listOptions.fieldSelector',
                     "metadata.namespace=%s" % config["namespace"])]
    if labels is not None:
        query_params.append((
            'listOptions.labelSelector',
            ",".join(["%s=%s" % (k, v) for k, v in labels.items()])))
    with get_argo_api_client() as api_client:
        res = api_client.call_api(
            '/api/v1/archived-workflows',
            'GET', response_type='object',
            header_params=config["http_headers"],
            query_params=query_params,
            _return_http_data_only=True)
    return [ArgoWorkflow(w) for w in res["items"]] if res["items"] else []
