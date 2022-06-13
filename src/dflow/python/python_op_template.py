import inspect, uuid, jsonpickle, random, string

from .opio import Artifact, BigParameter
from ..op_template import PythonScriptOPTemplate
from ..io import Inputs, Outputs, InputParameter, OutputParameter, InputArtifact, OutputArtifact, S3Artifact
from ..utils import upload_artifact
from ..client import V1alpha1RetryStrategy
upload_packages = []

class PythonOPTemplate(PythonScriptOPTemplate):
    """
    Convert from Python class OP to OP template

    Args:
        op_class: Python class OP
        image: image of the OP template
        command: python executable
        input_artifact_slices: a dict specifying input artifacts to use slices
        output_artifact_save: a dict specifying storage of output artifacts overriding default storage
        output_artifact_archive: a dict specifying compress format of output artifacts, None for no compression
        input_parameter_slices: a dict specifying input parameters to use slices
        output_artifact_slices: a dict specifying output artifacts to use slices
        output_parameter_slices: a dict specifying output parameters to use slices
        output_artifact_global_name: a dict specifying global names of output artifacts within the workflow
        slices: use slices to generate parallel steps
        python_packages: local python packages to be uploaded to the OP
        timeout: timeout of the OP template
        retry_on_transient_error: maximum retries on TrasientError
        output_parameter_default: a dict specifying default values for output parameters
        output_parameter_global_name: a dict specifying global names of output parameters within the workflow
        timeout_as_transient_error: regard timeout as transient error or fatal one
        memoize_key: memoized key of the OP template
        volumes: volumes to use in the OP template
        mounts: volumes to mount in the OP template
        image_pull_policy: Always, IfNotPresent, Never
        cpu_requests: CPU requests
        memory_requests: memory requests
        cpu_limits: CPU limits
        memory_limits: memory limits
    """
    def __init__(self, op_class, image=None, command=None, input_artifact_slices=None, output_artifact_save=None,
                 output_artifact_archive=None, input_parameter_slices=None, output_artifact_slices=None,
                 output_parameter_slices=None, output_artifact_global_name=None, slices=None, python_packages=None,
                 timeout=None, retry_on_transient_error=None, output_parameter_default=None, output_parameter_global_name=None,
                 timeout_as_transient_error=False, memoize_key=None, volumes=None, mounts=None, image_pull_policy=None,
                 cpu_requests=None, memory_requests=None, cpu_limits=None, memory_limits=None):
        class_name = op_class.__name__
        input_sign = op_class.get_input_sign()
        output_sign = op_class.get_output_sign()
        if slices is not None:
            assert isinstance(slices, Slices)
            if slices.input_artifact is not None: input_artifact_slices = {name: slices.slices for name in slices.input_artifact}
            if slices.input_parameter is not None: input_parameter_slices = {name: slices.slices for name in slices.input_parameter}
            if slices.output_artifact is not None:
                output_artifact_slices = {}
                for name in slices.output_artifact:
                    output_artifact_slices[name] = slices.slices
                    output_sign[name].archive = None # not archive for default
            if slices.output_parameter is not None: output_parameter_slices = {name: slices.slices for name in slices.output_parameter}
        if output_artifact_save is not None:
            for name, save in output_artifact_save.items():
                output_sign[name].save = save
        if output_artifact_archive is not None:
            for name, archive in output_artifact_archive.items():
                output_sign[name].archive = archive
        if output_artifact_global_name is not None:
            for name, global_name in output_artifact_global_name.items():
                output_sign[name].global_name = global_name
        super().__init__(name="%s-%s" % (class_name, "".join(random.sample(string.digits + string.ascii_lowercase, 5))), inputs=Inputs(), outputs=Outputs(),
                volumes=volumes, mounts=mounts, cpu_requests=cpu_requests, memory_requests=memory_requests, cpu_limits=cpu_limits, memory_limits=memory_limits)
        self.slices = slices
        if timeout is not None: self.timeout = "%ss" % timeout
        if retry_on_transient_error is not None:
            if timeout_as_transient_error:
                expr = "asInt(lastRetry.exitCode) != 2"
            else:
                expr = "asInt(lastRetry.exitCode) == 1"
            self.retry_strategy = V1alpha1RetryStrategy(limit=retry_on_transient_error, expression=expr)
        self.dflow_vars = {}
        for name, sign in input_sign.items():
            if isinstance(sign, Artifact):
                self.inputs.artifacts[name] = InputArtifact(path="/tmp/inputs/artifacts/" + name, optional=sign.optional, type=sign.type)
            elif isinstance(sign, BigParameter):
                self.inputs.parameters[name] = InputParameter(save_as_artifact=True, path="/tmp/inputs/parameters/" + name, type=sign.type)
            else:
                self.inputs.parameters[name] = InputParameter(type=sign)
        for name, sign in output_sign.items():
            if isinstance(sign, Artifact):
                self.outputs.artifacts[name] = OutputArtifact(path="/tmp/outputs/artifacts/" + name,
                    archive=sign.archive, save=sign.save, global_name=sign.global_name, type=sign.type)
            elif isinstance(sign, BigParameter):
                self.outputs.parameters[name] = OutputParameter(save_as_artifact=True, value_from_path="/tmp/outputs/parameters/" + name, type=sign.type)
            else:
                default = None
                if output_parameter_default is not None and name in output_parameter_default:
                    if sign == str:
                        default = output_parameter_default[name]
                    else:
                        default = jsonpickle.dumps(output_parameter_default[name])
                global_name = None
                if output_parameter_global_name is not None and name in output_parameter_global_name:
                    global_name = output_parameter_global_name[name]
                self.outputs.parameters[name] = OutputParameter(value_from_path="/tmp/outputs/parameters/" + name, default=default, global_name=global_name, type=sign)

        if python_packages is None:
            python_packages = upload_packages
        elif isinstance(python_packages, list):
            python_packages = upload_packages + python_packages
        else:
            python_packages = upload_packages + [python_packages]

        script = ""
        if python_packages:
            self.inputs.artifacts["dflow_python_packages"] = InputArtifact(path="/tmp/inputs/artifacts/dflow_python_packages",
                    source=upload_artifact(python_packages))
            script += "from dflow.python.utils import handle_python_packages\n"
            script += "handle_python_packages('/tmp')\n"

        if op_class.__module__ == "__main__":
            source_lines, start_line = inspect.getsourcelines(op_class)
            with open(inspect.getsourcefile(op_class), "r") as fd:
                pre_lines = fd.readlines()[:start_line-1]
            script += "".join(pre_lines + source_lines) + "\n"

        script += "import os, sys, traceback, jsonpickle\n"
        script += "from dflow.python import OPIO, TransientError, FatalError\n"
        script += "from dflow.python.utils import handle_input_artifact, handle_input_parameter\n"
        script += "from dflow.python.utils import handle_output_artifact, handle_output_parameter\n"
        script += "from %s import %s\n\n" % (op_class.__module__, class_name)
        script += "op_obj = %s()\n" % class_name
        script += "input = OPIO()\n"
        script += "input_sign = %s.get_input_sign()\n" % class_name
        for name, sign in input_sign.items():
            if isinstance(sign, Artifact):
                slices = self.get_slices(input_artifact_slices, name)
                script += "input['%s'] = handle_input_artifact('%s', input_sign['%s'], %s, '/tmp')\n" % (name, name, name, slices)
            else:
                slices = self.get_slices(input_parameter_slices, name)
                if isinstance(sign, BigParameter):
                    script += "input['%s'] = handle_input_parameter('%s', '', input_sign['%s'], %s, '/tmp')\n" % (name, name, name, slices)
                else:
                    script += "input['%s'] = handle_input_parameter('%s', r'{{inputs.parameters.%s}}', input_sign['%s'], %s, '/tmp')\n" % (name, name, name, name, slices)

        script += "try:\n"
        script += "    output = op_obj.execute(input)\n"
        script += "except TransientError:\n"
        script += "    traceback.print_exc()\n"
        script += "    sys.exit(1)\n"
        script += "except FatalError:\n"
        script += "    traceback.print_exc()\n"
        script += "    sys.exit(2)\n"

        script += "os.makedirs('/tmp/outputs/parameters', exist_ok=True)\n"
        script += "os.makedirs('/tmp/outputs/artifacts', exist_ok=True)\n"
        script += "output_sign = %s.get_output_sign()\n" % class_name
        for name, sign in output_sign.items():
            if isinstance(sign, Artifact):
                slices = self.get_slices(output_artifact_slices, name)
                if slices is not None:
                    self.outputs.parameters["dflow_%s_path_list" % name] = OutputParameter(value_from_path="/tmp/outputs/parameters/dflow_%s_path_list" % name)
                script += "handle_output_artifact('%s', output['%s'], output_sign['%s'], %s, '/tmp')\n" % (name, name, name, slices)
            else:
                slices = self.get_slices(output_parameter_slices, name)
                script += "handle_output_parameter('%s', output['%s'], output_sign['%s'], %s, '/tmp')\n" % (name, name, name, slices)

        self.image = image
        self.image_pull_policy = image_pull_policy
        if command is not None:
            self.command = command
        else:
            self.command = ["python"]
        self.script = script
        self.init_progress = "%s/%s" % (op_class.progress_current, op_class.progress_total)

        self.memoize_key = memoize_key

    def get_slices(self, slices_dict, name):
        slices = None
        if slices_dict is not None:
            slices = self.render_slices(slices_dict.get(name, None))
        return slices

    def render_slices(self, slices=None):
        if slices is None:
            return None

        i = slices.find("{{item")
        while i >= 0:
            j = slices.find("}}", i+2)
            var = slices[i:j+2]
            if var not in self.dflow_vars:
                var_name = "dflow_var_%s" % len(self.dflow_vars)
                self.inputs.parameters[var_name] = InputParameter(value=var)
                self.dflow_vars[var] = var_name
            else:
                var_name = self.dflow_vars[var]
            slices = slices.replace(var, "{{inputs.parameters.%s}}" % var_name)
            i = slices.find("{{item")
        return slices

class Slices:
    """
    Slices specified in PythonOPTemplate

    Args:
        slices: slice pattern
        input_parameter: list of input parameters to be sliced
        input_artifact: list of input artifacts to be sliced
        output_parameter: list of output parameters to be stacked
        output_artifact: list of output artifacts to be stacked
    """
    def __init__(self, slices=None, input_parameter=None, input_artifact=None, output_parameter=None, output_artifact=None):
        self.slices = slices
        self.input_parameter = input_parameter
        self.input_artifact = input_artifact
        self.output_parameter = output_parameter
        self.output_artifact = output_artifact

class TransientError(Exception):
    pass

class FatalError(Exception):
    pass
