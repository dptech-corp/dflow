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
from .io import Inputs, Outputs, InputParameter, S3Artifact

class OPTemplate:
    def __init__(self, name, inputs=None, outputs=None, memoize_key=None, key=None, pvcs=None):
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
        if pvcs is None:
            pvcs = []
        self.pvcs = pvcs

    def handle_key(self, memoize_prefix=None, memoize_configmap="dflow-config"):
        if self.key is not None:
            self.inputs.parameters["dflow_key"] = InputParameter(value="")
            if memoize_prefix is not None:
                self.memoize_key = "%s-{{inputs.parameters.dflow_key}}" % memoize_prefix

            if hasattr(self, "slices") and self.slices is not None and self.slices.output_artifact is not None:
                self.inputs.parameters["dflow_group_key"] = InputParameter(value="")
                for name in self.slices.output_artifact:
                    for save in self.outputs.artifacts[name].save:
                        if isinstance(save, S3Artifact):
                            save.key = "{{workflow.name}}/{{inputs.parameters.dflow_group_key}}-%s" % name

        if self.memoize_key is not None:
            # Is it a bug of Argo?
            if self.memoize_key.find("workflow.name") != -1:
                self.inputs.parameters["workflow_name"] = InputParameter(value="{{workflow.name}}")
                self.memoize_key = self.memoize_key.replace("workflow.name", "inputs.parameters.workflow_name")
            config = Configuration()
            config.client_side_validation = False
            self.memoize = V1alpha1Memoize(key=self.memoize_key, local_vars_configuration=config, cache=V1alpha1Cache(config_map=V1ConfigMapKeySelector(name=memoize_configmap, local_vars_configuration=config)))

class ScriptOPTemplate(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None, volumes=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, key=None, pvcs=None):
        """
        Instantiate a script OP template
        :param name: the name of the OP template
        :param inputs: input parameters and input artifacts
        :param outputs: output parameters and output artifacts
        :param image: image the template uses
        :param command: command to run the script
        :param script: script
        :param volumes: volumes the template uses
        :param mounts: volumes the template mounts
        :param init_progress: a str representing the initial progress
        :param timeout: timeout of the OP template
        :param retry_strategy: retry strategy of the OP template
        :param memoize_key: memoized key of the OP template
        :param key: key of the OP template
        :param pvcs: PVCs need to be declared
        :return:
        """
        super().__init__(name=name, inputs=inputs, outputs=outputs, memoize_key=memoize_key, key=key, pvcs=pvcs)
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

    def convert_to_argo(self, memoize_prefix=None, memoize_configmap="dflow-config"):
        self.handle_key(memoize_prefix, memoize_configmap)
        return V1alpha1Template(name=self.name,
            metadata=V1alpha1Metadata(annotations={"workflows.argoproj.io/progress": self.init_progress}),
            inputs=self.inputs.convert_to_argo(),
            outputs=self.outputs.convert_to_argo(),
            timeout=self.timeout,
            retry_strategy=self.retry_strategy,
            memoize=self.memoize,
            volumes=self.volumes,
            script=V1alpha1ScriptTemplate(image=self.image, command=self.command, source=self.script, volume_mounts=self.mounts))

class ShellOPTemplate(ScriptOPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None, volumes=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, key=None, pvcs=None):
        """
        Instantiate a shell script OP template
        :param name: the name of the OP template
        :param inputs: input parameters and input artifacts
        :param outputs: output parameters and output artifacts
        :param image: image the template uses
        :param command: command to run the script
        :param script: shell script
        :param volumes: volumes the template uses
        :param mounts: volumes the template mounts
        :param init_progress: a str representing the initial progress
        :param timeout: timeout of the OP template
        :param retry_strategy: retry strategy of the OP template
        :param memoize_key: memoized key of the OP template
        :param key: key of the OP template
        :param pvcs: PVCs need to be declared
        :return:
        """
        if command is None:
            command = ["sh"]
        super().__init__(name=name, inputs=inputs, outputs=outputs, image=image, command=command, script=script, volumes=volumes,
                mounts=mounts, init_progress=init_progress, timeout=timeout, retry_strategy=retry_strategy, memoize_key=memoize_key,
                key=key, pvcs=pvcs)

class PythonScriptOPTemplate(ScriptOPTemplate):
    def __init__(self, name, inputs=None, outputs=None, image=None, command=None, script=None, volumes=None, mounts=None,
            init_progress="0/1", timeout=None, retry_strategy=None, memoize_key=None, key=None, pvcs=None):
        """
        Instantiate a python script OP template
        :param name: the name of the OP template
        :param inputs: input parameters and input artifacts
        :param outputs: output parameters and output artifacts
        :param image: image the template uses
        :param command: command to run the script
        :param script: python script
        :param volumes: volumes the template uses
        :param mounts: volumes the template mounts
        :param init_progress: a str representing the initial progress
        :param timeout: timeout of the OP template
        :param retry_strategy: retry strategy of the OP template
        :param memoize_key: memoized key of the OP template
        :param key: key of the OP template
        :param pvcs: PVCs need to be declared
        :return:
        """
        if command is None:
            command = ["python"]
        super().__init__(name=name, inputs=inputs, outputs=outputs, image=image, command=command, script=script, volumes=volumes,
                mounts=mounts, init_progress=init_progress, timeout=timeout, retry_strategy=retry_strategy, memoize_key=memoize_key,
                key=key, pvcs=pvcs)
