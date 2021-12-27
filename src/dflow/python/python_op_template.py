import inspect
from .opio import Artifact
from ..op_template import PythonScriptOPTemplate
from ..io import Inputs, Outputs, InputParameter, OutputParameter, InputArtifact, OutputArtifact

class PythonOPTemplate(PythonScriptOPTemplate):
    def __init__(self, op_class, image=None, command=None):
        class_name = op_class.__name__
        input_sign = op_class.get_input_sign()
        output_sign = op_class.get_output_sign()
        inputs = Inputs()
        outputs = Outputs()
        dflow_vars = {}
        for name, sign in input_sign.items():
            if isinstance(sign, Artifact):
                inputs.artifacts[name] = InputArtifact(path="/tmp/inputs/artifacts/" + name, optional=sign.optional)
            else:
                inputs.parameters[name] = InputParameter()
        for name, sign in output_sign.items():
            if isinstance(sign, Artifact):
                outputs.artifacts[name] = OutputArtifact(path="/tmp/outputs/artifacts/" + name,
                    archive=sign.archive, save=sign.save)
            else:
                outputs.parameters[name] = OutputParameter(value_from_path="/tmp/outputs/parameters/" + name)

        script = ""
        if op_class.__module__ == "__main__":
            source_lines, start_line = inspect.getsourcelines(op_class)
            pre_lines = open(inspect.getsourcefile(op_class), "r").readlines()[:start_line-1]
            script += "".join(pre_lines + source_lines) + "\n"

        script += "import jsonpickle\n"
        script += "from dflow.python import OPIO\n"
        script += "from dflow.python.utils import handle_input_artifact, handle_output\n"
        script += "from %s import %s\n\n" % (op_class.__module__, class_name)
        script += "op_obj = %s()\n" % class_name
        script += "input = OPIO()\n"
        script += "input_sign = %s.get_input_sign()\n" % class_name
        for name, sign in input_sign.items():
            if isinstance(sign, Artifact):
                if sign.slices is not None:
                    i = sign.slices.find("{{item")
                    while i >= 0:
                        j = sign.slices.find("}}", i+2)
                        var = sign.slices[i:j+2]
                        if var not in dflow_vars:
                            var_name = "dflow_var_%s" % len(dflow_vars)
                            inputs.parameters[var_name] = InputParameter(value=var)
                            dflow_vars[var] = var_name
                        else:
                            var_name = dflow_vars[var]
                        sign.slices = sign.slices.replace(var, "{{inputs.parameters.%s}}" % var_name)
                        i = sign.slices.find("{{item")
                script += "input['%s'] = handle_input_artifact('%s', input_sign['%s'], %s)\n" % (name, name, name, sign.slices)
            elif sign == str:
                script += "input['%s'] = '{{inputs.parameters.%s}}'\n" % (name, name)
            else:
                script += "input['%s'] = jsonpickle.loads('{{inputs.parameters.%s}}')\n" % (name, name)
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
