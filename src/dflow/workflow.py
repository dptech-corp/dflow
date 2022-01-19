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

    def submit(self, backend="argo"):
        if backend == "debug":
            return self.entrypoint.run()

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

        manifest = V1alpha1Workflow(
            metadata=V1ObjectMeta(generate_name=self.name + '-'),
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

    def handle_template(self, template):
        if template.name not in self.argo_templates:
            if isinstance(template, Steps): # if the template is steps, handle involved templates
                argo_template, templates = template.convert_to_argo() # breadth first algorithm
                self.argo_templates[template.name] = argo_template
                for template in templates:
                    self.handle_template(template)
            else:
                self.argo_templates[template.name] = template.convert_to_argo()
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

    def query_step(self, name=None):
        return self.query().get_step(name=name)
