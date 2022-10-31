from .common import S3Artifact
from .config import config
from .io import (InputArtifact, InputParameter, Inputs, OutputArtifact,
                 OutputParameter)
from .op_template import PythonScriptOPTemplate, ShellOPTemplate
from .utils import s3_config


class InitArtifactForSlices(PythonScriptOPTemplate):
    def __init__(self, name, image, command, image_pull_policy, key,
                 sliced_output_artifact, sub_path, sliced_input_artifact,
                 tmp_root="/tmp"):
        super().__init__(name="%s-init-artifact" % name, image=image,
                         command=command, image_pull_policy=image_pull_policy)
        self.key = key
        self.sliced_output_artifact = sliced_output_artifact
        self.sub_path = sub_path
        self.sliced_input_artifact = sliced_input_artifact
        self.tmp_root = tmp_root

        if self.key is not None:
            self.inputs.parameters["dflow_group_key"] = InputParameter()
            # For the case of reusing sliced steps, ensure that the output
            # artifacts are reused
            for name in self.sliced_output_artifact:
                self.outputs.artifacts[name] = OutputArtifact(
                    path="%s/outputs/artifacts/%s" % (self.tmp_root, name),
                    save=S3Artifact(key="%s{{workflow.name}}/{{inputs."
                                    "parameters.dflow_group_key}}/%s"
                                    % (s3_config["prefix"], name)),
                    archive=None)
        else:
            self.outputs.parameters["dflow_artifact_key"] = OutputParameter(
                value="%s{{workflow.name}}/{{pod.name}}" %
                s3_config["prefix"])
            for name in self.sliced_output_artifact:
                self.outputs.artifacts[name] = OutputArtifact(
                    path="%s/outputs/artifacts/%s" % (self.tmp_root, name),
                    save=S3Artifact(key="%s{{workflow.name}}/{{pod.name}}/%s"
                                    % (s3_config["prefix"], name)),
                    archive=None)

        if self.sub_path and self.sliced_input_artifact:
            for name in self.sliced_input_artifact:
                self.inputs.artifacts[name] = InputArtifact(
                    path="%s/inputs/artifacts/%s" % (self.tmp_root, name),
                    optional=True, sub_path=config["catalog_dir_name"])
                self.outputs.parameters["dflow_slices_path"] = OutputParameter(
                    value_from_path="%s/outputs/parameters/dflow_slices_path"
                    % self.tmp_root, type=str(dict))

        self.render_script()

    def render_script(self):
        script = "import os, json\n"
        for name in self.sliced_output_artifact:
            script += "os.makedirs('%s/outputs/artifacts/%s/%s', "\
                "exist_ok=True)\n" % (self.tmp_root, name,
                                      config["catalog_dir_name"])
            script += "with open('%s/outputs/artifacts/%s/%s/init',"\
                " 'w') as f:\n" % (self.tmp_root, name,
                                   config["catalog_dir_name"])
            script += "    json.dump({'path_list': []}, f)\n"

        if self.sub_path and self.sliced_input_artifact:
            for i, name in enumerate(self.sliced_input_artifact):
                script += "path_list_%s = []\n" % i
                script += "path = '%s/inputs/artifacts/%s'\n" % \
                    (self.tmp_root, name)
                script += "if os.path.exists(path):\n"
                script += "    for f in os.listdir(path):\n"
                script += "        with open(os.path.join(path, f), 'r')"\
                    " as fd:\n"
                script += "            path_list_%s += json.load(fd)"\
                    "['path_list']\n" % i
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
            script += "os.makedirs('%s/outputs/parameters', exist_ok=True)\n"\
                % self.tmp_root
            script += "with open('%s/outputs/parameters/dflow_slices_path',"\
                " 'w') as f:\n" % self.tmp_root
            script += "    json.dump(slices_path, f)\n"
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
