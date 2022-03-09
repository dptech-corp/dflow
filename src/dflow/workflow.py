from argo.workflows.client import (
    ApiClient, Configuration,
    WorkflowServiceApi,
    V1alpha1Workflow,
    V1alpha1WorkflowCreateRequest,
    V1alpha1WorkflowSpec,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1ResourceRequirements,
    V1alpha1ArchiveStrategy,
    V1alpha1WorkflowStatus,
    V1alpha1Outputs,
    V1alpha1Artifact
)
from .steps import Steps
from .argo_objects import ArgoWorkflow
from .utils import copy_s3
from .io import S3Artifact
import random, string, json
import kubernetes

class Workflow:
    def __init__(self, name="workflow", steps=None, id=None, host="https://127.0.0.1:2746", token=None, k8s_config_file=None):
        self.host = host
        self.token = token
        self.k8s_config_file = k8s_config_file

        configuration = Configuration(host=host)
        configuration.verify_ssl = False
        if token is None:
            api_client = ApiClient(configuration)
        else:
            api_client = ApiClient(configuration, header_name='Authorization', header_value='Bearer %s' % token)

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
        self.entrypoint.add(step)

    def submit(self, backend="argo", reuse_step=None):
        if backend == "debug":
            return self.entrypoint.run()

        status = None
        if reuse_step is not None:
            self.id = self.name + "-" + randstr()
            data = {}
            global_output_artifacts = {}
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
                            if hasattr(art, "globalName") and hasattr(art, "s3"):
                                key = art.s3.key
                                if hasattr(step, "inputs") and hasattr(step.inputs, "parameters") and "dflow_group_key" in step.inputs.parameters:
                                    key = "%s/%s-%s" % (self.id, step.inputs.parameters["dflow_group_key"].value, name)
                                    if art.s3.key not in copied_keys:
                                        copy_s3(art.s3.key, key)
                                        copied_keys.append(art.s3.key)
                                if hasattr(art, "archive"):
                                    archive = V1alpha1ArchiveStrategy(_none={})
                                else:
                                    archive = None
                                if art.globalName not in global_output_artifacts:
                                    global_output_artifacts[art.globalName] = V1alpha1Artifact(name=art.globalName, s3=S3Artifact(key=key), archive=archive)
                        outputs["artifacts"] = eval(str(list(step.outputs.artifacts.values())))
                data["%s-%s" % (self.id, step.key)] = json.dumps({
                    "nodeID": step.id,
                    "outputs": outputs,
                    "creationTimestamp": step.finishedAt,
                    "lastHitTimestamp": step.finishedAt
                })
            cm_name = "dflow-" + randstr()
            config_map = kubernetes.client.V1ConfigMap(data=data, metadata=kubernetes.client.V1ObjectMeta(name=cm_name))
            kubernetes.config.load_kube_config(config_file=self.k8s_config_file)
            v1 = kubernetes.client.CoreV1Api()
            v1.create_namespaced_config_map(namespace="argo", body=config_map)
            if len(global_output_artifacts) > 0:
                status = V1alpha1WorkflowStatus(outputs=V1alpha1Outputs(artifacts=list(global_output_artifacts.values())))
            self.handle_template(self.entrypoint, memoize_prefix=self.id, memoize_configmap=cm_name)
        else:
            self.handle_template(self.entrypoint)

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
                volume_claim_templates=argo_pvcs),
            status=status)

        response = self.api_instance.api_client.call_api('/api/v1/workflows/argo', 'POST',
                body=V1alpha1WorkflowCreateRequest(workflow=manifest), response_type=object,
                _return_http_data_only=True)
        workflow = ArgoWorkflow(response)

        self.id = workflow.metadata.name
        print("Workflow has been submitted (ID: %s)" % self.id)
        return workflow

    def handle_template(self, template, memoize_prefix=None, memoize_configmap="dflow-config"):
        if template.name in self.templates:
            assert template == self.templates[template.name], "Duplication of template name: %s" % template.name
        else:
            self.templates[template.name] = template
            if isinstance(template, Steps): # if the template is steps, handle involved templates
                argo_template, templates = template.convert_to_argo(memoize_prefix, memoize_configmap) # breadth first algorithm
                self.argo_templates[template.name] = argo_template
                for template in templates:
                    self.handle_template(template, memoize_prefix, memoize_configmap)
            else:
                self.argo_templates[template.name] = template.convert_to_argo(memoize_prefix, memoize_configmap)
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

    def query_step(self, name=None, key=None, phase=None, id=None):
        return self.query().get_step(name=name, key=key, phase=phase, id=id)

    def query_keys_of_steps(self):
        return [step.key for step in self.query_step() if step.key is not None]

def randstr(l=5):
    return "".join(random.sample(string.digits + string.ascii_lowercase, l))
