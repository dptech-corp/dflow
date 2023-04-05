from .common import S3Artifact
from .config import config
from .io import (InputArtifact, InputParameter, Inputs, OutputArtifact,
                 OutputParameter)
from .op_template import PythonScriptOPTemplate, ShellOPTemplate


class InitArtifactForSlices(PythonScriptOPTemplate):
    def __init__(self, name, image, command, image_pull_policy, key,
                 sliced_output_artifact, sliced_input_artifact, sum_var,
                 concat_var, tmp_root="/tmp"):
        super().__init__(name="%s-init-artifact" % name, image=image,
                         command=command, image_pull_policy=image_pull_policy)
        self.key = key
        self.sliced_output_artifact = sliced_output_artifact
        self.sliced_input_artifact = sliced_input_artifact
        self.sum_var = sum_var
        self.concat_var = concat_var
        self.tmp_root = tmp_root

        if self.key is not None:
            self.inputs.parameters["dflow_group_key"] = InputParameter()
            # For the case of reusing sliced steps, ensure that the output
            # artifacts are reused
            for name in self.sliced_output_artifact:
                self.outputs.artifacts[name] = OutputArtifact(
                    path="%s/outputs/artifacts/%s" % (self.tmp_root, name),
                    save=S3Artifact(key="{{workflow.name}}/{{inputs."
                                    "parameters.dflow_group_key}}/%s" % name),
                    archive=None)
            self.outputs.parameters["dflow_artifact_key"] = OutputParameter(
                value="{{workflow.name}}/"
                "{{inputs.parameters.dflow_group_key}}")
        else:
            self.outputs.parameters["dflow_artifact_key"] = OutputParameter(
                value="{{workflow.name}}/{{pod.name}}")
            for name in self.sliced_output_artifact:
                self.outputs.artifacts[name] = OutputArtifact(
                    path="%s/outputs/artifacts/%s" % (self.tmp_root, name),
                    save=S3Artifact(key="{{workflow.name}}/{{pod.name}}/%s"
                                    % name),
                    archive=None)

        if self.sliced_input_artifact:
            if self.key is not None:
                self.inputs.parameters["dflow_key"] = InputParameter()
                self.outputs.parameters["dflow_global"] = OutputParameter(
                    value="{{pod.name}}",
                    global_name="{{inputs.parameters.dflow_key}}",
                )
            for name in self.sliced_input_artifact:
                self.inputs.artifacts[name] = InputArtifact(
                    path="%s/inputs/artifacts/%s" % (self.tmp_root, name),
                    optional=True, sub_path=config["catalog_dir_name"])
                self.outputs.parameters["dflow_slices_path"] = OutputParameter(
                    value_from_path="%s/outputs/parameters/dflow_slices_path"
                    % self.tmp_root, type=dict)

        if self.sum_var is not None:
            name = self.sum_var.name
            self.inputs.parameters[name] = InputParameter()
            self.outputs.parameters["sum_%s" % name] = OutputParameter(
                value_from_path="%s/outputs/parameters/sum_%s" %
                (self.tmp_root, name), type=int)

        if self.concat_var is not None:
            name = self.concat_var.name
            self.inputs.parameters[name] = InputParameter()
            self.outputs.parameters["concat_%s" % name] = OutputParameter(
                value_from_path="%s/outputs/parameters/concat_%s" %
                (self.tmp_root, name), type=list)

        self.render_script()

    def render_script(self):
        script = "import os, json\n"
        for name in self.sliced_output_artifact:
            script += "os.makedirs(r'%s/outputs/artifacts/%s/%s', "\
                "exist_ok=True)\n" % (self.tmp_root, name,
                                      config["catalog_dir_name"])
            script += "with open(r'%s/outputs/artifacts/%s/%s/init',"\
                " 'w') as f:\n" % (self.tmp_root, name,
                                   config["catalog_dir_name"])
            script += "    json.dump({'path_list': []}, f)\n"

        if self.sliced_input_artifact:
            for i, name in enumerate(self.sliced_input_artifact):
                script += "path_list_%s = []\n" % i
                script += "path = r'%s/inputs/artifacts/%s'\n" % \
                    (self.tmp_root, name)
                script += "if os.path.exists(path):\n"
                script += "    for f in os.listdir(path):\n"
                script += "        with open(os.path.join(path, f), 'r')"\
                    " as fd:\n"
                script += "            for i in json.load(fd)['path_list']:\n"
                script += "                if i not in path_list_%s:\n" % i
                script += "                    path_list_%s.append(i)\n" % i
                script += "path_list_%s.sort(key=lambda x: x['order'])\n" \
                    % i

            n_arts = len(self.sliced_input_artifact)
            if n_arts > 1:
                script += "assert " + " == ".join(
                    ["len(path_list_%s)" % i for i in range(n_arts)]) + "\n"

            script += "slices_path = []\n"
            script += "for i in range(len(path_list_0)):\n"
            script += "    item = {'order': i}\n"
            for i, name in enumerate(self.sliced_input_artifact):
                script += "    item['%s'] = path_list_%s[i]"\
                    "['dflow_list_item']\n" % (name, i)
            script += "    slices_path.append(item)\n"
            script += "os.makedirs(r'%s/outputs/parameters', exist_ok=True)\n"\
                % self.tmp_root
            script += "with open(r'%s/outputs/parameters/dflow_slices_path',"\
                " 'w') as f:\n" % self.tmp_root
            script += "    json.dump(slices_path, f)\n"

        if self.sum_var is not None:
            name = self.sum_var.name
            script += """value = r'{{inputs.parameters.%s}}'
if "dflow_list_item" in value:
    dflow_list = []
    for item in json.loads(value):
        dflow_list += json.loads(item)
    var = list(map(lambda x: x['dflow_list_item'], dflow_list))
else:
    var = json.loads(value)
os.makedirs(r'%s/outputs/parameters', exist_ok=True)
with open(r'%s/outputs/parameters/sum_%s', 'w') as f:
    f.write(str(sum(map(int, var))))\n""" % (
                name, self.tmp_root, self.tmp_root, name)

        if self.concat_var is not None:
            name = self.concat_var.name
            script += """value = r'{{inputs.parameters.%s}}'
var = []
if "dflow_list_item" in value:
    for item in json.loads(value):
        for i in json.loads(item):
            var += i['dflow_list_item']
else:
    for item in json.loads(value):
        if isinstance(item, str):
            var += json.loads(item)
        else:
            var += item
os.makedirs(r'%s/outputs/parameters', exist_ok=True)
with open(r'%s/outputs/parameters/concat_%s', 'w') as f:
    f.write(json.dumps(var))\n""" % (
                name, self.tmp_root, self.tmp_root, name)

        self.script = script


class CheckNumSuccess(ShellOPTemplate):
    def __init__(self, name="check-num-success", image=None,
                 image_pull_policy=None):
        super().__init__(name=name, image=image,
                         image_pull_policy=image_pull_policy)
        self.command = ["sh"]
        self.script = "succ=`echo {{inputs.parameters.success}} | grep -o 1 "\
            "| wc -l`\n"
        self.script += "exit $(( $succ < {{inputs.parameters.threshold}} ))"
        self.inputs = Inputs(
            parameters={"success": InputParameter(),
                        "threshold": InputParameter()})


class CheckSuccessRatio(ShellOPTemplate):
    def __init__(self, name="check-success-ratio", image=None,
                 image_pull_policy=None):
        super().__init__(name=name, image=image,
                         image_pull_policy=image_pull_policy)
        self.command = ["sh"]
        self.script = "succ=`echo {{inputs.parameters.success}} | grep -o 1 |"\
            " wc -l`\n"
        self.script += "exit `echo $succ {{inputs.parameters.total}} | awk -v"\
            " r={{inputs.parameters.threshold}} '{if ($1 < $2*r)"\
            " {print 1} else {print 0}}'`"
        self.inputs = Inputs(
            parameters={"success": InputParameter(),
                        "total": InputParameter(),
                        "threshold": InputParameter()})
