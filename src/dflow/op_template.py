from argo.workflows.client import (
    V1alpha1Template,
    V1alpha1ScriptTemplate,
    V1VolumeMount,
    V1alpha1Metadata,
    V1ConfigMapKeySelector,
    V1alpha1Memoize,
    V1alpha1Cache
)
from argo.workflows.client.configuration import Configuration
from .io import Inputs, Outputs, InputParameter

class OPTemplate:
    def __init__(self, name, inputs=None, outputs=None, memoize_key=None, key=None):
        self.name = name
        if inputs is not None:
            self.inputs = inputs
        else:
            self.inputs = Inputs()
        if outputs is not None:
            self.outputs = outputs
        else:
            self.outputs = Outputs()
        self.memoize_key = memoize_key
        self.key = key
        self.memoize = None

    def handle_key(self, memoize_prefix=None):
        if self.key is not None:
            self.inputs.parameters["dflow_key"] = InputParameter(value=str(self.key))
            if memoize_prefix is not None:
                self.memoize_key = "%s-{{inputs.parameters.dflow_key}}" % memoize_prefix

        if self.memoize_key is not None:
            # Is it a bug of Argo?
            if self.memoize_key.find("workflow.name") != -1:
                self.inputs.parameters["workflow_name"] = InputParameter(value="{{workflow.name}}")
                self.memoize_key = self.memoize_key.replace("workflow.name", "inputs.parameters.workflow_name")
            config = Configuration()
            config.client_side_validation = False
            self.memoize = V1alpha1Memoize(key=self.memoize_key, local_vars_configuration=config, cache=V1alpha1Cache(config_map=V1ConfigMapKeySelector(name="dflow-config", local_vars_configuration=config)))

class ScriptOPTemplate(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, key=None):
        super().__init__(name=name, inputs=inputs, outputs=outputs, memoize_key=memoize_key, key=key)
        self.image = image
        self.command = command
        self.script = script
        if mounts is None:
            mounts = []
        self.mounts = mounts
        self.init_progress = init_progress
        self.timeout = timeout
        self.retry_strategy = retry_strategy

    def convert_to_argo(self, memoize_prefix=None):
        self.handle_key(memoize_prefix)
        return V1alpha1Template(name=self.name,
            metadata=V1alpha1Metadata(annotations={"workflows.argoproj.io/progress": self.init_progress}),
            inputs=self.inputs.convert_to_argo(),
            outputs=self.outputs.convert_to_argo(),
            timeout=self.timeout,
            retry_strategy=self.retry_strategy,
            memoize=self.memoize,
            script=V1alpha1ScriptTemplate(image=self.image, command=self.command, source=self.script, volume_mounts=self.mounts))

class ShellOPTemplate(ScriptOPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, key=None):
        if command is None:
            command = ["sh"]
        super().__init__(name=name, inputs=inputs, outputs=outputs, image=image, command=command, script=script,
                mounts=mounts, init_progress=init_progress, timeout=timeout, retry_strategy=retry_strategy, memoize_key=memoize_key, key=key)

class PythonScriptOPTemplate(ScriptOPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, key=None):
        if command is None:
            command = ["python"]
        super().__init__(name=name, inputs=inputs, outputs=outputs, image=image, command=command, script=script,
                mounts=mounts, init_progress=init_progress, timeout=timeout, retry_strategy=retry_strategy, memoize_key=memoize_key, key=key)
