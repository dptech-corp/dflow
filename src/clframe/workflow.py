from argo.workflows.client import (
    ApiClient, Configuration,
    WorkflowServiceApi,
    V1alpha1Template,
    V1alpha1Workflow,
    V1alpha1WorkflowCreateRequest,
    V1alpha1WorkflowSpec,
    V1ObjectMeta,
    V1alpha1Parameter,
    V1alpha1Inputs,
    V1alpha1Outputs,
    V1alpha1ValueFrom,
    V1alpha1Artifact
)
from .steps import Steps

class Workflow:
    def __init__(self, name, steps=None, ip="127.0.0.1", port=2746):
        self.ip = ip
        self.port = port
        self.name = name
        if steps is not None:
            self.entrypoint = steps
        else:
            self.entrypoint = Steps(self.name)
        self.argo_templates = {}

    def add(self, step):
        self.entrypoint.add(step)

    def submit(self):
        self.handle_template(self.entrypoint)

        configuration = Configuration(host="https://%s:%s" % (self.ip, self.port))
        configuration.verify_ssl = False
        api_client = ApiClient(configuration)
        api_instance = WorkflowServiceApi(api_client)

        manifest = V1alpha1Workflow(
            metadata=V1ObjectMeta(generate_name=self.name + '-'),
            spec=V1alpha1WorkflowSpec(
                service_account_name='argo',
                entrypoint=self.entrypoint.name,
                templates=list(self.argo_templates.values())))

        api_response = api_instance.create_workflow(
            namespace='argo',
            body=V1alpha1WorkflowCreateRequest(workflow=manifest))
        print(api_response)
        return api_response

    def handle_step(self, step):
        self.handle_template(step.template)
        return step.convert_to_argo()

    def handle_template(self, template):
        if template.name not in self.argo_templates:
            self.argo_templates[template.name] = None # placeholder, in case infinite loop
            if isinstance(template, Steps): # if the template is steps, handle it recursively
                argo_steps = []
                for step in template:
                    # each step of steps should be a list of parallel steps, if not, create a sigleton
                    if not isinstance(step, list):
                        step = [step]
                    argo_steps.append([self.handle_step(ps) for ps in step])

                self.argo_templates[template.name] = V1alpha1Template(name=template.name,
                    steps=argo_steps,
                    inputs=V1alpha1Inputs(parameters=[
                        V1alpha1Parameter(name=k, value=v.value) for k, v in template.inputs.parameters.items()],
                        artifacts=[V1alpha1Artifact(name=k, path=v.path, _from=v.source) for k, v in template.inputs.artifacts.items()]),
                    outputs=V1alpha1Outputs(parameters=[
                        V1alpha1Parameter(name=k, value_from=
                        V1alpha1ValueFrom(path=v.value_from_path)) for k, v in template.outputs.parameters.items()],
                        artifacts=[V1alpha1Artifact(name=k, path=v.path) for k, v in template.outputs.artifacts.items()]))
            else:
                self.argo_templates[template.name] = template.convert_to_argo()
