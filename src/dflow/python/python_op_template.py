import inspect
import os
import random
import string
from pathlib import Path
from typing import Optional, Any, Dict, List, Union

import jsonpickle
import typeguard

from .. import __path__
from ..common import S3Artifact
from ..config import config
from ..io import (PVC, InputArtifact, InputParameter, Inputs, OutputArtifact,
                  OutputParameter, Outputs)
from ..op_template import PythonScriptOPTemplate
from ..utils import s3_config
from .op import OP
from .opio import Artifact, BigParameter, Parameter

try:
    from argo.workflows.client import (V1alpha1UserContainer, V1Volume,
                                       V1VolumeMount)

    from ..client import V1alpha1RetryStrategy
except Exception:
    V1Volume = object
    V1VolumeMount = object
    V1alpha1UserContainer = object
upload_packages = []


class Slices:
    """
    Slices specified in PythonOPTemplate

    Args:
        slices: slice pattern
        input_parameter: list of input parameters to be sliced
        input_artifact: list of input artifacts to be sliced
        output_parameter: list of output parameters to be stacked
        output_artifact: list of output artifacts to be stacked
        group_size: number of slices per task/step
        pool_size: for multi slices per task/step, use a multiprocessing pool
            to handle each slice, 1 for serial, -1 for infinity (i.e. equals to
            the number of slices)
        register_first_only: only register first slice when lineage used
    """

    def __init__(
            self,
            slices: Optional[str] = None,
            input_parameter: Optional[List[str]] = None,
            input_artifact: Optional[List[str]] = None,
            output_parameter: Optional[List[str]] = None,
            output_artifact: Optional[List[str]] = None,
            sub_path: bool = False,
            group_size: Optional[int] = None,
            pool_size: Optional[int] = None,
            register_first_only: bool = False,
    ) -> None:
        self.input_parameter = input_parameter if input_parameter is not \
            None else []
        self.input_artifact = input_artifact if input_artifact is not None \
            else []
        self.output_parameter = output_parameter if output_parameter is not \
            None else []
        self.output_artifact = output_artifact if output_artifact is not \
            None else []
        self.sub_path = sub_path
        if slices is not None:
            self.slices = slices
        elif self.sub_path:
            self.slices = "{{item.order}}"
        else:
            self.slices = "{{item}}"
        self.group_size = group_size
        self.pool_size = pool_size
        self.register_first_only = register_first_only


def get_source_code(o):
    source_lines, start_line = inspect.getsourcelines(o)
    with open(inspect.getsourcefile(o), "r",
              encoding="utf-8") as fd:
        pre_lines = fd.readlines()[:start_line-1]
    return "".join(pre_lines + source_lines) + "\n"


class PythonOPTemplate(PythonScriptOPTemplate):
    """
    Convert from Python class OP to OP template

    Args:
        op_class: Python class OP
        image: image of the OP template
        command: python executable
        input_artifact_slices: a dict specifying input artifacts to use slices
        output_artifact_save: a dict specifying storage of output artifacts
            overriding default storage
        output_artifact_archive: a dict specifying compress format of output
            artifacts, None for no compression
        input_parameter_slices: a dict specifying input parameters to use
            slices
        output_artifact_slices: a dict specifying output artifacts to use
            slices
        output_parameter_slices: a dict specifying output parameters to use
            slices
        output_artifact_global_name: a dict specifying global names of
            output artifacts within the workflow
        slices: use slices to generate parallel steps
        python_packages: local python packages to be uploaded to the OP
        timeout: timeout of the OP template
        retry_on_transient_error: maximum retries on TrasientError
        output_parameter_default: a dict specifying default values for output
            parameters
        output_parameter_global_name: a dict specifying global names of output
            parameters within the workflow
        timeout_as_transient_error: regard timeout as transient error or fatal
            one
        memoize_key: memoized key of the OP template
        volumes: volumes to use in the OP template
        mounts: volumes to mount in the OP template
        image_pull_policy: Always, IfNotPresent, Never
        requests: a dict of resource requests
        limits: a dict of resource limits
        envs: environment variables
        init_containers: init containers before the template runs
    """

    def __init__(self,
                 op_class: OP,
                 image: Optional[str] = None,
                 command: Union[str, List[str]] = None,
                 output_artifact_save: Dict[str,
                                            List[Union[PVC, S3Artifact]]]
                 = None,
                 output_artifact_archive: Dict[str, str] = None,
                 output_parameter_default: Dict[str, Any] = None,
                 input_artifact_slices: Dict[str, str] = None,
                 input_parameter_slices: Dict[str, str] = None,
                 output_artifact_slices: Dict[str, str] = None,
                 output_parameter_slices: Dict[str, str] = None,
                 output_artifact_global_name: Dict[str, str] = None,
                 output_parameter_global_name: Dict[str, str] = None,
                 slices: Optional[Slices] = None,
                 python_packages: Optional[List[os.PathLike]] = None,
                 timeout: Optional[int] = None,
                 retry_on_transient_error: Optional[int] = None,
                 timeout_as_transient_error: bool = False,
                 memoize_key: Optional[str] = None,
                 volumes: Optional[List[V1Volume]] = None,
                 mounts: Optional[List[V1VolumeMount]] = None,
                 image_pull_policy: Optional[str] = None,
                 requests: Dict[str, str] = None,
                 limits: Dict[str, str] = None,
                 upload_dflow: bool = True,
                 envs: Dict[str, str] = None,
                 init_containers: Optional[List[V1alpha1UserContainer]] = None,
                 tmp_root: str = "/tmp",
                 pre_script: str = "",
                 post_script: str = "",
                 ) -> None:
        self.n_parts = {}
        self.op_class = op_class
        op = None
        if isinstance(op_class, OP):
            op = op_class
            op_class = op.__class__
        class_name = op_class.__name__
        input_sign = op_class.get_input_sign()
        output_sign = op_class.get_output_sign()
        if output_artifact_save is not None:
            for name, save in output_artifact_save.items():
                output_sign[name].save = save
        if output_artifact_archive is not None:
            for name, archive in output_artifact_archive.items():
                output_sign[name].archive = archive
        if output_artifact_global_name is not None:
            for name, global_name in output_artifact_global_name.items():
                output_sign[name].global_name = global_name
        super().__init__(name="%s-%s" % (class_name, "".join(random.sample(
            string.digits + string.ascii_lowercase, 5))), inputs=Inputs(),
            outputs=Outputs(), volumes=volumes, mounts=mounts,
            requests=requests, limits=limits, envs=envs,
            init_containers=init_containers)
        self.pre_script = pre_script
        self.post_script = post_script
        if timeout is not None:
            self.timeout = "%ss" % timeout
        if retry_on_transient_error is not None:
            if timeout_as_transient_error:
                expr = "asInt(lastRetry.exitCode) != 2"
            else:
                expr = "asInt(lastRetry.exitCode) == 1"
            self.retry_strategy = V1alpha1RetryStrategy(
                limit=retry_on_transient_error, expression=expr)
        self.dflow_vars = {}
        self.tmp_root = tmp_root
        for name, sign in input_sign.items():
            if isinstance(sign, Artifact):
                self.inputs.artifacts[name] = InputArtifact(
                    path="%s/inputs/artifacts/" % self.tmp_root + name,
                    optional=sign.optional, type=sign.type,
                    archive=sign.archive)
            elif isinstance(sign, BigParameter):
                if hasattr(sign, "default"):
                    self.inputs.parameters[name] = InputParameter(
                        save_as_artifact=config["mode"] != "debug",
                        path="%s/inputs/parameters/" % self.tmp_root + name,
                        type=sign.type, value=sign.default)
                else:
                    self.inputs.parameters[name] = InputParameter(
                        save_as_artifact=config["mode"] != "debug",
                        path="%s/inputs/parameters/" % self.tmp_root + name,
                        type=sign.type)
            elif isinstance(sign, Parameter):
                if hasattr(sign, "default"):
                    self.inputs.parameters[name] = InputParameter(
                        type=sign.type, value=sign.default)
                else:
                    self.inputs.parameters[name] = InputParameter(
                        type=sign.type)
            else:
                self.inputs.parameters[name] = InputParameter(type=sign)
        for name, sign in output_sign.items():
            if isinstance(sign, Artifact):
                self.outputs.artifacts[name] = OutputArtifact(
                    path="%s/outputs/artifacts/" % self.tmp_root + name,
                    archive=sign.archive, save=sign.save,
                    global_name=sign.global_name, type=sign.type)
                if config["save_path_as_parameter"]:
                    self.outputs.parameters["dflow_%s_path_list" %
                                            name] = OutputParameter(
                        value_from_path="%s/outputs/parameters/"
                        "dflow_%s_path_list" % (self.tmp_root, name),
                        default=[])
                if config["lineage"]:
                    self.outputs.parameters["dflow_%s_urn" % name] = \
                        OutputParameter(
                        value_from_path="%s/outputs/parameters/dflow_%s_urn"
                        % (self.tmp_root, name), default="")
            elif isinstance(sign, BigParameter):
                self.outputs.parameters[name] = OutputParameter(
                    save_as_artifact=config["mode"] != "debug",
                    value_from_path="%s/outputs/parameters/" % self.tmp_root
                    + name, type=sign.type)
            elif isinstance(sign, Parameter):
                if hasattr(sign, "default"):
                    self.outputs.parameters[name] = OutputParameter(
                        value_from_path="%s/outputs/parameters/"
                        % self.tmp_root + name, default=sign.default,
                        global_name=sign.global_name, type=sign.type)
                else:
                    self.outputs.parameters[name] = OutputParameter(
                        value_from_path="%s/outputs/parameters/"
                        % self.tmp_root + name, global_name=sign.global_name,
                        type=sign.type)
            else:
                default = None
                if output_parameter_default is not None and name in \
                        output_parameter_default:
                    if sign == str:
                        default = output_parameter_default[name]
                    else:
                        default = jsonpickle.dumps(
                            output_parameter_default[name])
                global_name = None
                if output_parameter_global_name is not None and name in \
                        output_parameter_global_name:
                    global_name = output_parameter_global_name[name]
                self.outputs.parameters[name] = OutputParameter(
                    value_from_path="%s/outputs/parameters/" % self.tmp_root
                    + name, default=default, global_name=global_name,
                    type=sign)

        if python_packages is None:
            python_packages = upload_packages.copy()
        elif isinstance(python_packages, list):
            python_packages = upload_packages + python_packages
        else:
            python_packages = upload_packages + [python_packages]

        if upload_dflow:
            python_packages += __path__
            python_packages += jsonpickle.__path__
            python_packages += typeguard.__path__

        self.python_packages = None
        if python_packages:
            self.python_packages = set(python_packages)
            self.inputs.artifacts["dflow_python_packages"] = InputArtifact(
                path="%s/inputs/artifacts/dflow_python_packages"
                % self.tmp_root)

        self.image = image
        self.image_pull_policy = image_pull_policy
        if isinstance(command, str):
            self.command = [command]
        elif command is not None:
            self.command = command
        else:
            self.command = ["python3"]
        self.init_progress = "%s/%s" % (op_class.progress_current,
                                        op_class.progress_total)
        self.memoize_key = memoize_key

        self.op_class = op_class
        self.input_sign = input_sign
        self.output_sign = output_sign
        self.op = op
        self.input_artifact_slices = {} if input_artifact_slices is None \
            else input_artifact_slices
        self.input_parameter_slices = {} if input_parameter_slices is None \
            else input_parameter_slices
        self.output_artifact_slices = {} if output_artifact_slices is None \
            else output_artifact_slices
        self.output_parameter_slices = {} if output_parameter_slices is None \
            else output_parameter_slices
        self.slices = slices

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key == "slices":
            self.init_slices(value)
            self.render_script()

    def init_slices(self, slices):
        self.input_artifact_slices = {}
        self.input_parameter_slices = {}
        self.output_artifact_slices = {}
        self.output_parameter_slices = {}
        self.add_slices(slices)

    def add_slices(self, slices):
        if slices is not None:
            assert isinstance(slices, Slices)
            if slices.input_artifact and not slices.sub_path:
                for name in slices.input_artifact:
                    self.input_artifact_slices[name] = slices.slices
            if slices.input_parameter:
                for name in slices.input_parameter:
                    self.input_parameter_slices[name] = slices.slices
            if slices.output_artifact:
                self.output_artifact_slices = {}
                for name in slices.output_artifact:
                    self.output_artifact_slices[name] = slices.slices
                    self.outputs.artifacts[name].archive = None  # no archive
            if slices.output_parameter:
                for name in slices.output_parameter:
                    self.output_parameter_slices[name] = slices.slices

            if slices.sub_path:
                for name in slices.input_artifact:
                    self.inputs.parameters["dflow_%s_sub_path" %
                                           name] = InputParameter(value=".")
                    sign = self.input_sign[name]
                    self.inputs.artifacts[name] = InputArtifact(
                        path="%s/inputs/artifacts/%s/{{inputs.parameters."
                        "dflow_%s_sub_path}}" % (self.tmp_root, name, name),
                        optional=sign.optional, type=sign.type)

    def render_script(self):
        op_class = self.op_class
        class_name = op_class.__name__
        op = self.op
        input_sign = self.input_sign
        output_sign = self.output_sign
        input_artifact_slices = self.input_artifact_slices
        input_parameter_slices = self.input_parameter_slices
        output_artifact_slices = self.output_artifact_slices
        output_parameter_slices = self.output_parameter_slices

        script = self.pre_script
        if self.python_packages:
            script += "import os, sys, json\n"
            script += "package_root = r'%s/inputs/artifacts/"\
                "dflow_python_packages'\n" % self.tmp_root
            script += "catalog_dir = os.path.join(package_root, "\
                "'%s')\n" % config['catalog_dir_name']
            script += "if os.path.exists(catalog_dir):\n"
            script += "    for f in os.listdir(catalog_dir):\n"
            script += "        with open(os.path.join(catalog_dir, f), 'r')"\
                " as fd:\n"
            script += "            for item in json.load(fd)['path_list']:\n"
            script += "                sys.path.insert(0, os.path.join("\
                "package_root, os.path.dirname(item['dflow_list_item'])))\n"

        script += "import json, jsonpickle\n"
        script += "from dflow import config, s3_config\n"
        script += "config.update(jsonpickle.loads(r'''%s'''))\n" % \
            jsonpickle.dumps(config)
        script += "s3_config.update(jsonpickle.loads(r'''%s'''))\n" % \
            jsonpickle.dumps(s3_config)
        if op_class.__module__ in ["__main__", "__mp_main__"]:
            try:
                if hasattr(op_class, "func"):
                    script += get_source_code(op_class.func)
                else:
                    script += get_source_code(op_class)
            except Exception:
                import cloudpickle
                if self.python_packages:
                    self.python_packages.update(cloudpickle.__path__)
                else:
                    self.python_packages = set(cloudpickle.__path__)

                script += "import cloudpickle\n"
                if hasattr(op_class, "func"):
                    script += "from dflow.python import OP\n"
                    script += "%s = OP.function(cloudpickle.loads(%s))\n" % \
                        (class_name, cloudpickle.dumps(op_class.func))
                else:
                    script += "%s = cloudpickle.loads(%s)\n" % \
                        (class_name, cloudpickle.dumps(op_class))

        script += "import os, sys, traceback, jsonpickle\n"
        script += "from dflow.python import OPIO, TransientError, FatalError\n"
        script += "from dflow.python.utils import handle_input_artifact," \
                  " handle_input_parameter\n"
        script += "from dflow.python.utils import handle_output_artifact," \
                  " handle_output_parameter, handle_lineage\n"
        script += f"from {op_class.__module__} import {class_name}\n\n"
        if hasattr(op_class, "func"):
            script += "op_obj = %s\n" % class_name
        elif op is None:
            script += "op_obj = %s()\n" % class_name
        else:
            script += "op_obj = jsonpickle.loads(r'''%s''')\n" % \
                jsonpickle.dumps(op)
        script += "def try_to_execute(input):\n"
        script += "    try:\n"
        script += "        return op_obj.execute(input), None\n"
        script += "    except Exception as e:\n"
        script += "        traceback.print_exc()\n"
        script += "        return None, e\n\n"
        script += "if __name__ == '__main__':\n"
        script += "    input = OPIO()\n"
        script += "    input_sign = %s.get_input_sign()\n" % class_name
        script += "    output_sign = %s.get_output_sign()\n" % class_name
        if self.slices is not None and self.slices.pool_size is not None:
            script += "    from typing import List\n"
            script += "    from pathlib import Path\n"
            for name in self.slices.input_artifact:
                if isinstance(input_sign[name], Artifact):
                    if input_sign[name].type == str:
                        script += "    input_sign['%s'].type = List[str]\n" % \
                            name
                    elif input_sign[name].type == Path:
                        script += "    input_sign['%s'].type = List[Path]\n" %\
                            name
        for name, sign in input_sign.items():
            if isinstance(sign, Artifact):
                slices = self.get_slices(input_artifact_slices, name)
                if self.slices is not None and self.slices.sub_path and \
                        name in self.slices.input_artifact:
                    script += "    input['%s'] = handle_input_artifact('%s', "\
                        "input_sign['%s'], %s, r'%s', '{{inputs.parameters."\
                        "dflow_%s_sub_path}}', None)\n" % (
                            name, name, name, slices, self.tmp_root, name)
                else:
                    script += "    input['%s'] = handle_input_artifact('%s', "\
                        "input_sign['%s'], %s, r'%s', None, %s)\n" \
                        % (name, name, name, slices, self.tmp_root,
                           self.n_parts.get(name, None))
            else:
                slices = self.get_slices(input_parameter_slices, name)
                if isinstance(sign, BigParameter) and \
                        config["mode"] != "debug":
                    script += "    input['%s'] = handle_input_parameter('%s',"\
                        " '', input_sign['%s'], %s, r'%s')\n" \
                        % (name, name, name, slices, self.tmp_root)
                else:
                    script += "    input['%s'] = handle_input_parameter('%s',"\
                        " r'''{{inputs.parameters.%s}}''', input_sign['%s'], "\
                        "%s, r'%s')\n" % (name, name, name, name, slices,
                                          self.tmp_root)

        if self.slices is not None and self.slices.pool_size is not None:
            sliced_inputs = self.slices.input_artifact + \
                self.slices.input_parameter
            if len(sliced_inputs) > 1:
                script += "    assert %s\n" % " == ".join(
                    ["len(input['%s'])" % i for i in sliced_inputs])
            script += "    n_slices = len(input['%s'])\n" % \
                sliced_inputs[0]
            script += "    input_list = []\n"
            script += "    from copy import deepcopy\n"
            script += "    for i in range(n_slices):\n"
            script += "        input1 = deepcopy(input)\n"
            for name in sliced_inputs:
                script += "        input1['%s'] = list(input['%s'])[i]\n" % (
                    name, name)
            script += "        input_list.append(input1)\n"
            if self.slices.pool_size == 1:
                script += "    output_list = []\n"
                script += "    error_list = []\n"
                script += "    for input in input_list:\n"
                script += "        output, error = try_to_execute(input)\n"
                script += "        output_list.append(output)\n"
                script += "        error_list.append(error)\n"
            else:
                script += "    from multiprocessing import Pool\n"
                if self.slices.pool_size == -1:
                    script += "    pool = Pool(n_slices)\n"
                else:
                    script += "    pool = Pool(%s)\n" % \
                        self.slices.pool_size
                script += "    oe_list = pool.map(try_to_execute, "\
                    "input_list)\n"
                script += "    output_list = [oe[0] for oe in oe_list]\n"
                script += "    error_list = [oe[1] for oe in oe_list]\n"
            sliced_outputs = self.slices.output_artifact + \
                self.slices.output_parameter
            script += "    output = OPIO()\n"
            script += "    for o in output_list:\n"
            script += "        if o is not None:\n"
            script += "            output = o\n"
            for name in sliced_outputs:
                script += "    output['%s'] = [o['%s'] if o is not None"\
                    " else None for o in output_list]\n" % (name, name)
                if isinstance(output_sign[name], Artifact):
                    if output_sign[name].type == str:
                        script += "    output_sign['%s'].type = List[str]"\
                            "\n" % name
                    elif output_sign[name].type == Path:
                        script += "    output_sign['%s'].type = List[Path"\
                            "]\n" % name
        else:
            script += "    try:\n"
            script += "        output = op_obj.execute(input)\n"
            script += "    except TransientError:\n"
            script += "        traceback.print_exc()\n"
            script += "        sys.exit(1)\n"
            script += "    except FatalError:\n"
            script += "        traceback.print_exc()\n"
            script += "        sys.exit(2)\n"

        script += "    os.makedirs(r'%s/outputs/parameters', exist_ok=True)\n"\
            % self.tmp_root
        script += "    os.makedirs(r'%s/outputs/artifacts', exist_ok=True)\n" \
            % self.tmp_root
        for name, sign in output_sign.items():
            if isinstance(sign, Artifact):
                slices = self.get_slices(output_artifact_slices, name)
                script += "    handle_output_artifact('%s', output['%s'], "\
                    "output_sign['%s'], %s, r'%s')\n" % (name, name, name,
                                                         slices, self.tmp_root)
            else:
                slices = self.get_slices(output_parameter_slices, name)
                script += "    handle_output_parameter('%s', output['%s'], "\
                    "output_sign['%s'], %s, r'%s')\n" % (name, name, name,
                                                         slices, self.tmp_root)
        if config["lineage"]:
            if self.slices is not None and self.slices.register_first_only:
                if "{{item}}" in self.dflow_vars:
                    var_name = self.dflow_vars["{{item}}"]
                else:
                    var_name = "dflow_var_%s" % len(self.dflow_vars)
                    self.inputs.parameters[var_name] = InputParameter(
                        value="{{item}}")
                    self.dflow_vars["{{item}}"] = var_name
                self.inputs.parameters["dflow_first"] = InputParameter()
                if not hasattr(self, "first_var"):
                    self.first_var = "r'''{{inputs.parameters.dflow_first}}'''"
                script += "    if r'''{{inputs.parameters.%s}}''' == "\
                    "%s:\n" % (var_name, self.first_var)
            else:
                script += "    if True:\n"
            script += "        input_urns = {}\n"
            for name, sign in input_sign.items():
                if isinstance(sign, Artifact):
                    script += "        input_urns['%s'] = '{{inputs."\
                        "parameters.dflow_%s_urn}}'\n" % (name, name)
            script += "        handle_lineage('{{workflow.name}}', "\
                "'{{pod.name}}', op_obj, input_urns, '{{workflow.parameters."\
                "dflow_workflow_urn}}', r'%s')\n" % self.tmp_root

        if self.slices is not None and self.slices.pool_size is not None:
            script += "    try:\n"
            script += "        for error in error_list:\n"
            script += "            if error is not None:\n"
            script += "                raise error\n"
            script += "    except TransientError:\n"
            script += "        sys.exit(1)\n"
            script += "    except FatalError:\n"
            script += "        sys.exit(2)\n"

        script += self.post_script
        self.script = script

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


class TransientError(Exception):
    pass


class FatalError(Exception):
    pass
