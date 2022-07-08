from typing import Dict, List, Union

from .io import PVC, InputParameter, Inputs, Outputs
from .utils import randstr

try:
    from argo.workflows.client import (V1alpha1Cache, V1alpha1Memoize,
                                       V1alpha1Metadata,
                                       V1alpha1ResourceTemplate,
                                       V1alpha1ScriptTemplate,
                                       V1alpha1Template,
                                       V1ConfigMapKeySelector,
                                       V1ResourceRequirements, V1Volume,
                                       V1VolumeMount)
    from argo.workflows.client.configuration import Configuration

    from .client.v1alpha1_retry_strategy import V1alpha1RetryStrategy
except Exception:
    V1alpha1ResourceTemplate = object
    V1alpha1RetryStrategy = object
    V1Volume = object
    V1VolumeMount = object


class OPTemplate:
    def __init__(
            self,
            name: str = None,
            inputs: Inputs = None,
            outputs: Outputs = None,
            memoize_key: str = None,
            pvcs: List[PVC] = None,
            annotations: Dict[str, str] = None,
            **kwargs,
    ) -> None:
        if name is None:
            name = randstr()
        # force lowercase to fix RFC 1123
        self.name = name.lower()
        if inputs is not None:
            self.inputs = inputs
        else:
            self.inputs = Inputs(template=self)
        if outputs is not None:
            self.outputs = outputs
        else:
            self.outputs = Outputs(template=self)
        self.memoize_key = memoize_key
        self.memoize = None
        if pvcs is None:
            pvcs = []
        self.pvcs = pvcs
        if annotations is None:
            annotations = {}
        self.annotations = annotations

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key == "inputs":
            assert isinstance(value, Inputs)
            value.set_template(self)
        elif key == "outputs":
            assert isinstance(value, Outputs)
            value.set_template(self)

    def handle_key(self, memoize_prefix=None,
                   memoize_configmap="dflow-config"):
        if "dflow_key" in self.inputs.parameters:
            if memoize_prefix is not None:
                self.memoize_key = "%s-{{inputs.parameters.dflow_key}}" \
                    % memoize_prefix

        if self.memoize_key is not None:
            # Is it a bug of Argo?
            if self.memoize_key.find("workflow.name") != -1:
                self.inputs.parameters["workflow_name"] = InputParameter(
                    value="{{workflow.name}}")
                self.memoize_key = self.memoize_key.replace(
                    "workflow.name", "inputs.parameters.workflow_name")
            config = Configuration()
            config.client_side_validation = False
            self.memoize = V1alpha1Memoize(
                key=self.memoize_key,
                local_vars_configuration=config, cache=V1alpha1Cache(
                    config_map=V1ConfigMapKeySelector(
                        name=memoize_configmap,
                        local_vars_configuration=config)))


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
        requests: a dict of resource requests
        limits: a dict of resource limits
    """

    def __init__(
            self,
            name: str = None,
            inputs: Inputs = None,
            outputs: Outputs = None,
            memoize_key: str = None,
            pvcs: List[PVC] = None,
            annotations: Dict[str, str] = None,
            image: str = None,
            command: Union[str, List[str]] = None,
            script: str = None,
            volumes: List[V1Volume] = None,
            mounts: List[V1VolumeMount] = None,
            init_progress: str = "0/1",
            timeout: str = None,
            retry_strategy: V1alpha1RetryStrategy = None,
            resource: V1alpha1ResourceTemplate = None,
            image_pull_policy: str = None,
            requests: Dict[str, str] = None,
            limits: Dict[str, str] = None,
            **kwargs,
    ) -> None:
        super().__init__(name=name, inputs=inputs, outputs=outputs,
                         memoize_key=memoize_key, pvcs=pvcs,
                         annotations=annotations)
        self.image = image
        if isinstance(command, str):
            command = [command]
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
        self.requests = requests
        self.limits = limits

    def convert_to_argo(self, memoize_prefix=None,
                        memoize_configmap="dflow-config"):
        self.handle_key(memoize_prefix, memoize_configmap)
        self.annotations["workflows.argoproj.io/progress"] = self.init_progress
        if self.resource is not None:
            return V1alpha1Template(name=self.name,
                                    metadata=V1alpha1Metadata(
                                        annotations=self.annotations),
                                    inputs=self.inputs.convert_to_argo(),
                                    outputs=self.outputs.convert_to_argo(),
                                    timeout=self.timeout,
                                    retry_strategy=self.retry_strategy,
                                    memoize=self.memoize,
                                    volumes=self.volumes,
                                    resource=self.resource)
        else:
            return \
                V1alpha1Template(name=self.name,
                                 metadata=V1alpha1Metadata(
                                     annotations=self.annotations),
                                 inputs=self.inputs.convert_to_argo(),
                                 outputs=self.outputs.convert_to_argo(),
                                 timeout=self.timeout,
                                 retry_strategy=self.retry_strategy,
                                 memoize=self.memoize,
                                 volumes=self.volumes,
                                 script=V1alpha1ScriptTemplate(
                                     image=self.image,
                                     image_pull_policy=self.image_pull_policy,
                                     command=self.command, source=self.script,
                                     volume_mounts=self.mounts,
                                     resources=V1ResourceRequirements(
                                         limits=self.limits,
                                         requests=self.requests)))


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
        requests: a dict of resource requests
        limits: a dict of resource limits
    """

    def __init__(
        self,
        name: str = None,
        inputs: Inputs = None,
        outputs: Outputs = None,
        memoize_key: str = None,
        pvcs: List[PVC] = None,
        annotations: Dict[str, str] = None,
        image: str = None,
        command: Union[str, List[str]] = None,
        script: str = None,
        volumes: List[V1Volume] = None,
        mounts: List[V1VolumeMount] = None,
        init_progress: str = "0/1",
        timeout: str = None,
        retry_strategy: V1alpha1RetryStrategy = None,
        image_pull_policy: str = None,
        requests: Dict[str, str] = None,
        limits: Dict[str, str] = None,
        **kwargs,
    ) -> None:
        if command is None:
            command = ["sh"]
        super().__init__(
            name=name, inputs=inputs, outputs=outputs, image=image,
            command=command, script=script, volumes=volumes, mounts=mounts,
            init_progress=init_progress, timeout=timeout,
            retry_strategy=retry_strategy, memoize_key=memoize_key, pvcs=pvcs,
            image_pull_policy=image_pull_policy, annotations=annotations,
            requests=requests, limits=limits)


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
        requests: a dict of resource requests
        limits: a dict of resource limits
    """

    def __init__(
        self,
        name: str = None,
        inputs: Inputs = None,
        outputs: Outputs = None,
        memoize_key: str = None,
        pvcs: List[PVC] = None,
        annotations: Dict[str, str] = None,
        image: str = None,
        command: Union[str, List[str]] = None,
        script: str = None,
        volumes: List[V1Volume] = None,
        mounts: List[V1VolumeMount] = None,
        init_progress: str = "0/1",
        timeout: str = None,
        retry_strategy: V1alpha1RetryStrategy = None,
        image_pull_policy: str = None,
        requests: Dict[str, str] = None,
        limits: Dict[str, str] = None,
        **kwargs,
    ) -> None:
        if command is None:
            command = ["python"]
        super().__init__(
            name=name, inputs=inputs, outputs=outputs, image=image,
            command=command, script=script, volumes=volumes, mounts=mounts,
            init_progress=init_progress, timeout=timeout,
            retry_strategy=retry_strategy, memoize_key=memoize_key, pvcs=pvcs,
            image_pull_policy=image_pull_policy, annotations=annotations,
            requests=requests, limits=limits)
