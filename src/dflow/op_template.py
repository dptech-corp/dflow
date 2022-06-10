from argo.workflows.client import (
    V1alpha1Template,
    V1alpha1ScriptTemplate,
    V1alpha1Metadata,
    V1ConfigMapKeySelector,
    V1alpha1Memoize,
    V1alpha1Cache,
    V1ResourceRequirements
)
from argo.workflows.client.configuration import Configuration
from .io import Inputs, Outputs, InputParameter
from .utils import randstr

class OPTemplate:
    def __init__(self, name=None, inputs=None, outputs=None, memoize_key=None, pvcs=None, annotations=None):
        if name is None:
            name = randstr()
        # force lowercase to fix RFC 1123
        self.name = name.lower()
        if inputs is not None:
            self.inputs = inputs
        else:
            self.inputs = Inputs()
        if outputs is not None:
            self.outputs = outputs
        else:
            self.outputs = Outputs()
        self.memoize_key = memoize_key
        self.memoize = None
        if pvcs is None:
            pvcs = []
        self.pvcs = pvcs
        if annotations is None:
            annotations = {}
        self.annotations = annotations

    def handle_key(self, memoize_prefix=None, memoize_configmap="dflow-config"):
        if "dflow_key" in self.inputs.parameters:
            if memoize_prefix is not None:
                self.memoize_key = "%s-{{inputs.parameters.dflow_key}}" % memoize_prefix

        if self.memoize_key is not None:
            # Is it a bug of Argo?
            if self.memoize_key.find("workflow.name") != -1:
                self.inputs.parameters["workflow_name"] = InputParameter(value="{{workflow.name}}")
                self.memoize_key = self.memoize_key.replace("workflow.name", "inputs.parameters.workflow_name")
            config = Configuration()
            config.client_side_validation = False
            self.memoize = V1alpha1Memoize(key=self.memoize_key, local_vars_configuration=config, cache=V1alpha1Cache(config_map=V1ConfigMapKeySelector(name=memoize_configmap, local_vars_configuration=config)))

class ScriptOPTemplate(OPTemplate):
    """
    Script OP template

    Args:
        name: the name of the OP template
        inputs: input parameters and input artifacts
        outputs: output parameters and output artifacts
        image: image the template uses
        command: command to run the script
        script: script
        volumes: volumes the template uses
        mounts: volumes the template mounts
        init_progress: a str representing the initial progress
        timeout: timeout of the OP template
        retry_strategy: retry strategy of the OP template
        memoize_key: memoized key of the OP template
        pvcs: PVCs need to be declared
        image_pull_policy: Always, IfNotPresent, Never
        annotations: annotations for the OP template
        cpu_requests: CPU requests
        memory_requests: memory requests
        cpu_limits: CPU limits
        memory_limits: memory limits
    """
    def __init__(self, name=None, inputs=None, outputs=None, image=None, command=None, script=None, volumes=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, pvcs=None, resource=None,
            image_pull_policy=None, annotations=None, cpu_requests=None, memory_requests=None, cpu_limits=None, memory_limits=None):
        super().__init__(name=name, inputs=inputs, outputs=outputs, memoize_key=memoize_key, pvcs=pvcs, annotations=annotations)
        self.image = image
        self.command = command
        self.script = script
        if volumes is None:
            volumes = []
        self.volumes = volumes
        if mounts is None:
            mounts = []
        self.mounts = mounts
        self.init_progress = init_progress
        self.timeout = timeout
        self.retry_strategy = retry_strategy
        self.resource = resource
        self.image_pull_policy = image_pull_policy
        self.cpu_requests = cpu_requests
        self.memory_requests = memory_requests
        self.cpu_limits = cpu_limits
        self.memory_limits = memory_limits

    def convert_to_argo(self, memoize_prefix=None, memoize_configmap="dflow-config"):
        self.handle_key(memoize_prefix, memoize_configmap)
        self.annotations["workflows.argoproj.io/progress"] = self.init_progress
        if self.resource is not None:
            return V1alpha1Template(name=self.name,
                metadata=V1alpha1Metadata(annotations=self.annotations),
                inputs=self.inputs.convert_to_argo(),
                outputs=self.outputs.convert_to_argo(),
                timeout=self.timeout,
                retry_strategy=self.retry_strategy,
                memoize=self.memoize,
                volumes=self.volumes,
                resource=self.resource)
        else:
            requests = {}
            if self.cpu_requests is not None:
                requests["cpu"] = self.cpu_requests
            if self.memory_requests is not None:
                requests["memory"] = self.memory_requests
            limits = {}
            if self.cpu_limits is not None:
                limits["cpu"] = self.cpu_limits
            if self.memory_limits is not None:
                limits["memory"] = self.memory_limits
            return V1alpha1Template(name=self.name,
                metadata=V1alpha1Metadata(annotations=self.annotations),
                inputs=self.inputs.convert_to_argo(),
                outputs=self.outputs.convert_to_argo(),
                timeout=self.timeout,
                retry_strategy=self.retry_strategy,
                memoize=self.memoize,
                volumes=self.volumes,
                script=V1alpha1ScriptTemplate(image=self.image, image_pull_policy=self.image_pull_policy,
                    command=self.command, source=self.script, volume_mounts=self.mounts, resources=V1ResourceRequirements(limits=limits, requests=requests)))

class ShellOPTemplate(ScriptOPTemplate):
    """
    Shell script OP template

    Args:
        name: the name of the OP template
        inputs: input parameters and input artifacts
        outputs: output parameters and output artifacts
        image: image the template uses
        command: command to run the script
        script: shell script
        volumes: volumes the template uses
        mounts: volumes the template mounts
        init_progress: a str representing the initial progress
        timeout: timeout of the OP template
        retry_strategy: retry strategy of the OP template
        memoize_key: memoized key of the OP template
        pvcs: PVCs need to be declared
        image_pull_policy: Always, IfNotPresent, Never
        annotations: annotations for the OP template
        cpu_requests: CPU requests
        memory_requests: memory requests
        cpu_limits: CPU limits
        memory_limits: memory limits
    """
    def __init__(self, name=None, inputs=None, outputs=None, image=None, command=None, script=None, volumes=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, pvcs=None, image_pull_policy=None,
            annotations=None, cpu_requests=None, memory_requests=None, cpu_limits=None, memory_limits=None):
        if command is None:
            command = ["sh"]
        super().__init__(name=name, inputs=inputs, outputs=outputs, image=image, command=command, script=script, volumes=volumes,
                mounts=mounts, init_progress=init_progress, timeout=timeout, retry_strategy=retry_strategy, memoize_key=memoize_key,
                pvcs=pvcs, image_pull_policy=image_pull_policy, annotations=annotations, cpu_requests=cpu_requests, memory_requests=memory_requests,
                cpu_limits=cpu_limits, memory_limits=memory_limits)

class PythonScriptOPTemplate(ScriptOPTemplate):
    """
    Python script OP template

    Args:
        name: the name of the OP template
        inputs: input parameters and input artifacts
        outputs: output parameters and output artifacts
        image: image the template uses
        command: command to run the script
        script: python script
        volumes: volumes the template uses
        mounts: volumes the template mounts
        init_progress: a str representing the initial progress
        timeout: timeout of the OP template
        retry_strategy: retry strategy of the OP template
        memoize_key: memoized key of the OP template
        pvcs: PVCs need to be declared
        image_pull_policy: Always, IfNotPresent, Never
        annotations: annotations for the OP template
        cpu_requests: CPU requests
        memory_requests: memory requests
        cpu_limits: CPU limits
        memory_limits: memory limits
    """
    def __init__(self, name=None, inputs=None, outputs=None, image=None, command=None, script=None, volumes=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, pvcs=None, image_pull_policy=None,
            annotations=None, cpu_requests=None, memory_requests=None, cpu_limits=None, memory_limits=None):
        if command is None:
            command = ["python"]
        super().__init__(name=name, inputs=inputs, outputs=outputs, image=image, command=command, script=script, volumes=volumes,
                mounts=mounts, init_progress=init_progress, timeout=timeout, retry_strategy=retry_strategy, memoize_key=memoize_key,
                pvcs=pvcs, image_pull_policy=image_pull_policy, annotations=annotations, cpu_requests=cpu_requests, memory_requests=memory_requests,
                cpu_limits=cpu_limits, memory_limits=memory_limits)
