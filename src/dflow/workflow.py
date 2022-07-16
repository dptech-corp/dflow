import json
import logging
import os
from typing import Dict, List, Union

import jsonpickle

from .argo_objects import ArgoStep, ArgoWorkflow
from .common import S3Artifact
from .config import config
from .context import Context
from .dag import DAG
from .step import Step
from .steps import Steps
from .task import Task
from .utils import copy_s3, randstr

try:
    import kubernetes
    import urllib3
    urllib3.disable_warnings()
    from argo.workflows.client import (ApiClient, Configuration,
                                       V1alpha1Workflow,
                                       V1alpha1WorkflowCreateRequest,
                                       V1alpha1WorkflowSpec, V1ObjectMeta,
                                       V1PersistentVolumeClaim,
                                       V1PersistentVolumeClaimSpec,
                                       V1ResourceRequirements,
                                       WorkflowServiceApi)
except Exception:
    pass


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
    """

    def __init__(
            self,
            name: str = "workflow",
            steps: Steps = None,
            dag: DAG = None,
            namespace: str = "argo",
            id: str = None,
            host: str = None,
            token: str = None,
            k8s_config_file: os.PathLike = None,
            k8s_api_server: str = None,
            context: Context = None,
            annotations: Dict[str, str] = None,
            parallelism: int = None
    ) -> None:
        self.host = host if host is not None else config["host"]
        self.token = token if token is not None else config["token"]
        self.k8s_config_file = k8s_config_file if k8s_config_file is not None \
            else config["k8s_config_file"]
        self.k8s_api_server = k8s_api_server if k8s_api_server is not None \
            else config["k8s_api_server"]
        self.context = context
        if annotations is None:
            annotations = {}
        self.annotations = annotations
        self.parallelism = parallelism

        configuration = Configuration(host=self.host)
        configuration.verify_ssl = False
        if self.token is None:
            api_client = ApiClient(configuration)
        else:
            api_client = ApiClient(
                configuration, header_name='Authorization',
                header_value='Bearer %s' % self.token)

        self.api_instance = WorkflowServiceApi(api_client)

        self.namespace = namespace
        if id is not None:
            self.id = id
        else:
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
            self.id = None

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
            reuse_step: List[ArgoStep] = None,
    ) -> ArgoWorkflow:
        """
        Submit the workflow

        Args:
            reuse_step: a list of steps to be reused in the workflow
        """
        assert self.id is None, "Do not submit a workflow repeatedly"

        manifest = self.convert_to_argo(reuse_step=reuse_step)

        response = self.api_instance.api_client.call_api(
            '/api/v1/workflows/%s' % self.namespace, 'POST',
            body=V1alpha1WorkflowCreateRequest(workflow=manifest),
            response_type=object,
            _return_http_data_only=True)
        workflow = ArgoWorkflow(response)

        self.id = workflow.metadata.name
        print("Workflow has been submitted (ID: %s)" % self.id)
        return workflow

    def convert_to_argo(self, reuse_step=None):
        if self.context is not None:
            assert isinstance(self.context, Context)
            self = self.context.render(self)

        status = None
        if reuse_step is not None:
            self.id = self.name + "-" + randstr()
            data = {}
            copied_keys = []
            for step in reuse_step:
                if step.key is None:
                    continue
                outputs = {}
                if hasattr(step, "outputs"):
                    if hasattr(step.outputs, "exitCode"):
                        outputs["exitCode"] = step.outputs.exitCode
                    if hasattr(step.outputs, "parameters"):
                        for name in list(step.outputs.parameters):
                            if hasattr(step.outputs.parameters[name],
                                       "save_as_artifact"):
                                del step.outputs.parameters[name]
                            elif not isinstance(
                                    step.outputs.parameters[name].value, str):
                                step.outputs.parameters[name].value = \
                                    jsonpickle.dumps(
                                        step.outputs.parameters[name].value)
                        outputs["parameters"] = [
                            par.recover()
                            for par in step.outputs.parameters.values()]
                    if hasattr(step.outputs, "artifacts"):
                        for name, art in step.outputs.artifacts.items():
                            if hasattr(step, "inputs") and \
                                hasattr(step.inputs, "parameters") and \
                                "dflow_group_key" in \
                                    step.inputs.parameters:
                                if hasattr(art, "s3") and \
                                        art.s3.key not in copied_keys:
                                    key = "%s/%s/%s" % (
                                        self.id,
                                        step.inputs.parameters[
                                            "dflow_group_key"].value, name)
                                    copy_s3(art.s3.key, key)
                                    copied_keys.append(art.s3.key)
                            if hasattr(art, "s3") and isinstance(art.s3,
                                                                 S3Artifact):
                                art.s3 = art.s3.to_dict()
                        outputs["artifacts"] = [
                            art.recover()
                            for art in step.outputs.artifacts.values()]
                data["%s-%s" % (self.id, step.key)] = json.dumps({
                    "nodeID": step.id,
                    "outputs": outputs,
                    "creationTimestamp": step.finishedAt,
                    "lastHitTimestamp": step.finishedAt
                })
            cm_name = "dflow-" + randstr()
            config_map = kubernetes.client.V1ConfigMap(
                data=data, metadata=kubernetes.client.V1ObjectMeta(
                    name=cm_name))
            if self.k8s_api_server is not None:
                k8s_configuration = kubernetes.client.Configuration(
                    host=self.k8s_api_server)
                k8s_configuration.verify_ssl = False
                if self.token is None:
                    k8s_client = kubernetes.client.ApiClient(k8s_configuration)
                else:
                    k8s_client = kubernetes.client.ApiClient(
                        k8s_configuration, header_name='Authorization',
                        header_value='Bearer %s' % self.token)
                v1 = kubernetes.client.CoreV1Api(k8s_client)
            else:
                kubernetes.config.load_kube_config(
                    config_file=self.k8s_config_file)
                v1 = kubernetes.client.CoreV1Api()
            v1.create_namespaced_config_map(namespace=self.namespace,
                                            body=config_map)
            self.handle_template(
                self.entrypoint, memoize_prefix=self.id,
                memoize_configmap=cm_name)
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
            metadata = V1ObjectMeta(name=self.id, annotations=self.annotations)
        else:
            metadata = V1ObjectMeta(
                generate_name=self.name + '-', annotations=self.annotations)

        return V1alpha1Workflow(
            metadata=metadata,
            spec=V1alpha1WorkflowSpec(
                service_account_name='argo',
                entrypoint=self.entrypoint.name,
                templates=list(self.argo_templates.values()),
                parallelism=self.parallelism,
                volume_claim_templates=argo_pvcs),
            status=status)

    def handle_template(self, template, memoize_prefix=None,
                        memoize_configmap="dflow-config"):
        if template.name in self.templates:
            assert template == self.templates[template.name], \
                "Duplication of template name: %s" % template.name
        else:
            logging.debug("handle template %s" % template.name)
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
    ) -> ArgoWorkflow:
        """
        Query the workflow from Argo

        Returns:
            an ArgoWorkflow object
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        try:
            response = self.api_instance.api_client.call_api(
                '/api/v1/workflows/%s/%s' % (self.namespace, self.id),
                'GET', response_type=object, _return_http_data_only=True)
        except Exception:
            response = self.api_instance.api_client.call_api(
                '/api/v1/archived-workflows/%s' % self.id,
                'GET', response_type=object, _return_http_data_only=True)
        workflow = ArgoWorkflow(response)
        return workflow

    def query_status(
            self,
    ) -> str:
        """
        Query the status of the workflow from Argo

        Returns:
            Pending, Running, Succeeded, Failed, Error, etc
        """
        workflow = self.query()
        if "phase" not in workflow.status:
            return "Pending"
        else:
            return workflow.status.phase

    def query_step(
            self,
            name: str = None,
            key: str = None,
            phase: str = None,
            id: str = None,
    ) -> List[ArgoStep]:
        """
        Query the existing steps of the workflow from Argo

        Args:
            name: filter by name of step, support regex
            key: filter by key of step
            phase: filter by phase of step
            id: filter by id of step
        Returns:
            a list of steps
        """
        return self.query().get_step(name=name, key=key, phase=phase, id=id)

    def query_keys_of_steps(
            self,
    ) -> List[str]:
        """
        Query the keys of existing steps of the workflow from Argo

        Returns:
            a list of keys
        """
        return [step.key for step in self.query_step() if step.key is not None]
