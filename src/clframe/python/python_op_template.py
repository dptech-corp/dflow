import inspect
from typing import Set
from pathlib import Path
from ..op_template import PythonScriptOPTemplate
from ..io import Inputs, Outputs, InputParameter, OutputParameter, InputArtifact, OutputArtifact

class PythonOPTemplate(PythonScriptOPTemplate):
    def __init__(self, op_class, image=None, command=None):
        name = op_class.__name__
        input_parameter_sign = op_class.get_input_parameter_sign()
        input_artifact_sign = op_class.get_input_artifact_sign()
        output_parameter_sign = op_class.get_output_parameter_sign()
        output_artifact_sign = op_class.get_output_artifact_sign()
        inputs = Inputs()
        outputs = Outputs()
        for par in input_parameter_sign:
            inputs.parameters[par] = InputParameter()
        for art in input_artifact_sign:
            inputs.artifacts[art] = InputArtifact(path="/tmp/inputs/artifacts/" + art)
        for par in output_parameter_sign:
            outputs.parameters[par] = OutputParameter(value_from_path="/tmp/outputs/parameters/" + par)
        for art in output_artifact_sign:
            outputs.artifacts[art] = OutputArtifact(path="/tmp/outputs/artifacts/" + art)

        script = ""
        if op_class.__module__ == "__main__":
            source_lines, start_line = inspect.getsourcelines(op_class)
            pre_lines = open(inspect.getsourcefile(op_class), "r").readlines()[:start_line-1]
            script += "\n".join(pre_lines + source_lines) + "\n"

        script += "from clframe.python import OPParameter, OPArtifact, handle_output\n"
        script += "from %s import %s\n\n" % (op_class.__module__, name)
        script += "op_obj = %s()\n" % name
        script += "input_parameter = OPParameter({"
        items = []
        for par, sign in input_parameter_sign.items():
            if sign in [int, float]:
                items.append("'%s': {{inputs.parameters.%s}}" % (par, par))
            else:
                items.append("'%s': '{{inputs.parameters.%s}}'" % (par, par))
        script += ", ".join(items)
        script += "})\n"
        script += "input_artifact = OPArtifact({"
        items = []
        for art, sign in input_artifact_sign.items():
            if sign == Set[Path]:
                items.append("'%s': set(['/tmp/inputs/artifacts/%s'])" % (art, art))
            else:
                items.append("'%s': '/tmp/inputs/artifacts/%s'" % (art, art))
        script += ", ".join(items)
        script += "})\n"
        script += "output_parameter, output_artifact = op_obj.execute(input_parameter, input_artifact)\n"
        script += "handle_output(output_parameter, output_artifact)\n"

        super().__init__(name=name, inputs=inputs, outputs=outputs)
        self.image = image
        if command is not None:
            self.command = command
        else:
            self.command = ["python"]
        self.script = script
        self.init_progress = "%s/%s" % (op_class.progress_current, op_class.progress_total)
