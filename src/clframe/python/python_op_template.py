import inspect
from typing import Set
from .opio import ArtifactPath
from ..op_template import PythonScriptOPTemplate
from ..io import Inputs, Outputs, InputParameter, OutputParameter, InputArtifact, OutputArtifact

class PythonOPTemplate(PythonScriptOPTemplate):
    def __init__(self, op_class, image=None, command=None):
        class_name = op_class.__name__
        input_sign = op_class.get_input_sign()
        output_sign = op_class.get_output_sign()
        inputs = Inputs()
        outputs = Outputs()
        for name, sign in input_sign.items():
            if sign in [ArtifactPath, Set[ArtifactPath]]:
                inputs.artifacts[name] = InputArtifact(path="/tmp/inputs/artifacts/" + name)
            else:
                inputs.parameters[name] = InputParameter()
        for name, sign in output_sign.items():
            if sign in [ArtifactPath, Set[ArtifactPath]]:
                outputs.artifacts[name] = OutputArtifact(path="/tmp/outputs/artifacts/" + name)
            else:
                outputs.parameters[name] = OutputParameter(value_from_path="/tmp/outputs/parameters/" + name)

        script = ""
        if op_class.__module__ == "__main__":
            source_lines, start_line = inspect.getsourcelines(op_class)
            pre_lines = open(inspect.getsourcefile(op_class), "r").readlines()[:start_line-1]
            script += "".join(pre_lines + source_lines) + "\n"

        script += "import jsonpickle\n"
        script += "from clframe.python import OPIO, handle_output\n"
        script += "from pathlib import Path\n"
        script += "from %s import %s\n\n" % (op_class.__module__, class_name)
        script += "op_obj = %s()\n" % class_name
        script += "input = OPIO({"
        items = []
        for name, sign in input_sign.items():
            if sign == ArtifactPath:
                items.append("'%s': Path('/tmp/inputs/artifacts/%s')" % (name, name))
            elif sign == Set[ArtifactPath]:
                items.append("'%s': set([Path('/tmp/inputs/artifacts/%s')])" % (name, name))
            elif sign == str:
                items.append("'%s': '{{inputs.parameters.%s}}'" % (name, name))
            else:
                items.append("'%s': jsonpickle.loads('{{inputs.parameters.%s}}')" % (name, name))
        script += ", ".join(items)
        script += "})\n"
        script += "output = op_obj.execute(input)\n"
        script += "handle_output(output, %s.get_output_sign())\n" % class_name

        super().__init__(name=class_name, inputs=inputs, outputs=outputs)
        self.image = image
        if command is not None:
            self.command = command
        else:
            self.command = ["python"]
        self.script = script
        self.init_progress = "%s/%s" % (op_class.progress_current, op_class.progress_total)
