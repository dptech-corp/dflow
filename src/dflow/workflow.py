from argo.workflows.client import (
    ApiClient, Configuration,
    WorkflowServiceApi,
    V1alpha1Workflow,
    V1alpha1WorkflowCreateRequest,
    V1alpha1WorkflowSpec,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1ResourceRequirements
)
from .steps import Steps
from .argo_objects import ArgoWorkflow
from .utils import copy_s3, randstr
import json
import kubernetes

config = {
    "host": "https://127.0.0.1:2746",
    "token": None,
    "k8s_config_file": None,
    "k8s_api_server": None
}

class Workflow:
    def __init__(self, name="workflow", steps=None, id=None, host=None, token=None, k8s_config_file=None,
            k8s_api_server=None, context=None, annotations=None):
        """
        Instantiate a workflow
        :param name: the name of the workflow
        :param steps: steps used as the entrypoint of the workflow, if not provided, a empty steps will be used
        :param id: workflow ID in Argo, you can provide it to track an existing workflow
        :param host: URL of the Argo server, will override global config
        :param token: request the Argo server with the token, will override global config
        :param k8s_config_file: Kubernetes configuration file for accessing API server, will override global config
        :param k8s_api_server: Url of kubernetes API server, will override global config
        :param context: context for the workflow
        :param annotations: annotations for the workflow
        :return:
        """
        self.host = host if host is not None else config["host"]
        self.token = token if token is not None else config["token"]
        self.k8s_config_file = k8s_config_file if k8s_config_file is not None else config["k8s_config_file"]
        self.k8s_api_server = k8s_api_server if k8s_api_server is not None else config["k8s_api_server"]
        self.context = context
        if annotations is None:
            annotations = {}
        self.annotations = annotations

        configuration = Configuration(host=self.host)
        configuration.verify_ssl = False
        if self.token is None:
            api_client = ApiClient(configuration)
        else:
            api_client = ApiClient(configuration, header_name='Authorization', header_value='Bearer %s' % self.token)

        self.api_instance = WorkflowServiceApi(api_client)

        if id is not None:
            self.id = id
        else:
            self.name = name
            if steps is not None:
                self.entrypoint = steps
            else:
                self.entrypoint = Steps(self.name + "-steps")
            self.templates = {}
            self.argo_templates = {}
            self.pvcs = {}
            self.id = None

    def add(self, step):
        """
        Add a step or a list of parallel steps to the workflow
        :param step: a step or a list of parallel steps to be added to the entrypoint of the workflow
        :return:
        """
        self.entrypoint.add(step)

    def submit(self, backend="argo", reuse_step=None):
        """
        Submit the workflow
        :param backend: "debug" for local run
        :param reuse_step: a list of steps to be reused in the workflow
        :return:
        """
        if backend == "debug":
            return self.entrypoint.run()

        manifest = self.convert_to_argo(reuse_step=reuse_step)

        response = self.api_instance.api_client.call_api('/api/v1/workflows/argo', 'POST',
                body=V1alpha1WorkflowCreateRequest(workflow=manifest), response_type=object,
                _return_http_data_only=True)
        workflow = ArgoWorkflow(response)

        self.id = workflow.metadata.name
        print("Workflow has been submitted (ID: %s)" % self.id)
        return workflow

    def convert_to_argo(self, reuse_step=None):
        if self.context is not None:
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
                        outputs["parameters"] = eval(str(list(step.outputs.parameters.values())))
                    if hasattr(step.outputs, "artifacts"):
                        for name, art in step.outputs.artifacts.items():
                            if hasattr(step, "inputs") and hasattr(step.inputs, "parameters") and "dflow_group_key" in step.inputs.parameters:
                                if hasattr(art, "s3") and art.s3.key not in copied_keys:
                                    key = "%s/%s/%s" % (self.id, step.inputs.parameters["dflow_group_key"].value, name)
                                    copy_s3(art.s3.key, key)
                                    copied_keys.append(art.s3.key)
                        outputs["artifacts"] = eval(str(list(step.outputs.artifacts.values())))
                data["%s-%s" % (self.id, step.key)] = json.dumps({
                    "nodeID": step.id,
                    "outputs": outputs,
                    "creationTimestamp": step.finishedAt,
                    "lastHitTimestamp": step.finishedAt
                })
            cm_name = "dflow-" + randstr()
            config_map = kubernetes.client.V1ConfigMap(data=data, metadata=kubernetes.client.V1ObjectMeta(name=cm_name))
            if self.k8s_api_server is not None:
                k8s_configuration = kubernetes.client.Configuration(host=self.k8s_api_server)
                k8s_configuration.verify_ssl = False
                if self.token is None:
                    k8s_client = kubernetes.client.ApiClient(k8s_configuration)
                else:
                    k8s_client = kubernetes.client.ApiClient(k8s_configuration, header_name='Authorization', header_value='Bearer %s' % self.token)
                v1 = kubernetes.client.CoreV1Api(k8s_client)
            else:
                kubernetes.config.load_kube_config(config_file=self.k8s_config_file)
                v1 = kubernetes.client.CoreV1Api()
            v1.create_namespaced_config_map(namespace="argo", body=config_map)
            self.handle_template(self.entrypoint, memoize_prefix=self.id, memoize_configmap=cm_name)
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
            metadata = V1ObjectMeta(generate_name=self.name + '-', annotations=self.annotations)

        return V1alpha1Workflow(
            metadata=metadata,
            spec=V1alpha1WorkflowSpec(
                service_account_name='argo',
                entrypoint=self.entrypoint.name,
                templates=list(self.argo_templates.values()),
                volume_claim_templates=argo_pvcs),
            status=status)

    def handle_template(self, template, memoize_prefix=None, memoize_configmap="dflow-config"):
        if template.name in self.templates:
            assert template == self.templates[template.name], "Duplication of template name: %s" % template.name
        else:
            self.templates[template.name] = template
            if isinstance(template, Steps): # if the template is steps, handle involved templates
                argo_template, templates = template.convert_to_argo(memoize_prefix, memoize_configmap, self.context) # breadth first algorithm
                self.argo_templates[template.name] = argo_template
                for template in templates:
                    self.handle_template(template, memoize_prefix, memoize_configmap)
            else:
                self.argo_templates[template.name] = template.convert_to_argo(memoize_prefix, memoize_configmap)
                for pvc in template.pvcs:
                    if pvc.name not in self.pvcs:
                        self.pvcs[pvc.name] = pvc

    def query(self):
        """
        Query the workflow from Argo
        :return: an ArgoWorkflow object
        """
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        response = self.api_instance.api_client.call_api('/api/v1/workflows/argo/%s' % self.id,
                'GET', response_type=object, _return_http_data_only=True)
        workflow = ArgoWorkflow(response)
        return workflow

    def query_status(self):
        """
        Query the status of the workflow from Argo
        :return: Pending, Running, Succeeded, Failed, Error, etc
        """
        workflow = self.query()
        if "phase" not in workflow.status:
            return "Pending"
        else:
            return workflow.status.phase

    def query_step(self, name=None, key=None, phase=None, id=None):
        """
        Query the existing steps of the workflow from Argo
        :param name: filter by name of step, support regex
        :param key: filter by key of step
        :param phase: filter by phase of step
        :param id: filter by id of step
        :return: a list of steps
        """
        return self.query().get_step(name=name, key=key, phase=phase, id=id)

    def query_keys_of_steps(self):
        """
        Query the keys of existing steps of the workflow from Argo
        :return: a list of keys
        """
        return [step.key for step in self.query_step() if step.key is not None]
