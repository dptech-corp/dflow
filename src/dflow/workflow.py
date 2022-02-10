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
import random, string, json
import kubernetes

class Workflow:
    def __init__(self, name="workflow", steps=None, ip="127.0.0.1", port=2746, id=None):
        self.ip = ip
        self.port = port

        configuration = Configuration(host="https://%s:%s" % (self.ip, self.port))
        configuration.verify_ssl = False
        api_client = ApiClient(configuration)
        self.api_instance = WorkflowServiceApi(api_client)

        if id is not None:
            self.id = id
        else:
            self.name = name
            if steps is not None:
                self.entrypoint = steps
            else:
                self.entrypoint = Steps(self.name + "-steps")
            self.argo_templates = {}
            self.pvcs = {}
            self.id = None

    def add(self, step):
        self.entrypoint.add(step)

    def dflow_config_exists(self, config_maps):
        for config_map in config_maps:
            if config_map.metadata.name == "dflow-config":
                return True
        return False

    def submit(self, backend="argo", reuse_step=None):
        if backend == "debug":
            return self.entrypoint.run()

        if reuse_step is not None:
            self.id = self.name + "-" + "".join(random.sample(string.digits + string.ascii_lowercase, 5))
            data = {}
            for step in reuse_step:
                data["%s-%s" % (self.id, step.key)] = json.dumps({
                    "nodeID": step.id,
                    "outputs": {
                        "parameters": eval(str(list(step.outputs.parameters.values()))),
                        "artifacts": eval(str(list(step.outputs.artifacts.values()))),
                        "exitCode": step.outputs.exitCode
                    },
                    "creationTimestamp": step.finishedAt,
                    "lastHitTimestamp": step.finishedAt
                })
            config_map = kubernetes.client.V1ConfigMap(data=data, metadata=kubernetes.client.V1ObjectMeta(name="dflow-config"))
            kubernetes.config.load_kube_config()
            v1 = kubernetes.client.CoreV1Api()
            config_maps = v1.list_namespaced_config_map(namespace="argo").items
            if not self.dflow_config_exists(config_maps):
                v1.create_namespaced_config_map(namespace="argo", body=config_map)
            else:
                v1.patch_namespaced_config_map(namespace="argo", name="dflow-config", body=config_map)

        self.handle_template(self.entrypoint, memoize_prefix=self.id if reuse_step is not None else None)

        argo_pvcs = []
        for k, v in self.pvcs.items():
            if v == "":
                argo_pvcs.append(V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(name=k),
                    spec=V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=V1ResourceRequirements(
                            requests={"storage": "1Gi"}
                        )
                    )
                ))

        if self.id is not None:
            metadata = V1ObjectMeta(name=self.id)
        else:
            metadata = V1ObjectMeta(generate_name=self.name + '-')

        manifest = V1alpha1Workflow(
            metadata=metadata,
            spec=V1alpha1WorkflowSpec(
                service_account_name='argo',
                entrypoint=self.entrypoint.name,
                templates=list(self.argo_templates.values()),
                volume_claim_templates=argo_pvcs))

        response = self.api_instance.api_client.call_api('/api/v1/workflows/argo', 'POST',
                body=V1alpha1WorkflowCreateRequest(workflow=manifest), response_type=object,
                _return_http_data_only=True)
        workflow = ArgoWorkflow(response)

        self.id = workflow.metadata.name
        print("Workflow has been submitted (ID: %s)" % self.id)
        return workflow

    def handle_template(self, template, memoize_prefix=None):
        if template.name not in self.argo_templates:
            if isinstance(template, Steps): # if the template is steps, handle involved templates
                argo_template, templates = template.convert_to_argo(memoize_prefix) # breadth first algorithm
                self.argo_templates[template.name] = argo_template
                for template in templates:
                    self.handle_template(template, memoize_prefix)
            else:
                self.argo_templates[template.name] = template.convert_to_argo(memoize_prefix)
                for mount in template.mounts:
                    if mount.name not in self.pvcs:
                        self.pvcs[mount.name] = ""

    def query(self):
        if self.id is None:
            raise RuntimeError("Workflow ID is None")
        response = self.api_instance.api_client.call_api('/api/v1/workflows/argo/%s' % self.id,
                'GET', response_type=object, _return_http_data_only=True)
        workflow = ArgoWorkflow(response)
        return workflow

    def query_status(self):
        workflow = self.query()
        if "phase" not in workflow.status:
            return "Pending"
        else:
            return workflow.status.phase

    def query_step(self, name=None, key=None):
        return self.query().get_step(name=name, key=key)

    def query_keys_of_steps(self):
        return [step.key for step in self.query_step() if step.key is not None]
