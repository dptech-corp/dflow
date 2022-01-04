import inspect
from .opio import Artifact
from ..op_template import PythonScriptOPTemplate
from ..io import Inputs, Outputs, InputParameter, OutputParameter, InputArtifact, OutputArtifact

class PythonOPTemplate(PythonScriptOPTemplate):
    def __init__(self, op_class, image=None, command=None, input_artifact_slices=None, output_artifact_save=None,
                 output_artifact_archive=None, input_parameter_slices=None):
        class_name = op_class.__name__
        input_sign = op_class.get_input_sign()
        output_sign = op_class.get_output_sign()
        if input_artifact_slices is not None:
            for name, slices in input_artifact_slices.items():
                input_sign[name].slices = slices
        if output_artifact_save is not None:
            for name, save in output_artifact_save.items():
                output_sign[name].save = save
        if output_artifact_archive is not None:
            for name, archive in output_artifact_archive.items():
                output_sign[name].archive = archive
        super().__init__(name=class_name, inputs=Inputs(), outputs=Outputs())
        self.dflow_vars = {}
        for name, sign in input_sign.items():
            if isinstance(sign, Artifact):
                self.inputs.artifacts[name] = InputArtifact(path="/tmp/inputs/artifacts/" + name, optional=sign.optional)
            else:
                self.inputs.parameters[name] = InputParameter()
        for name, sign in output_sign.items():
            if isinstance(sign, Artifact):
                self.outputs.artifacts[name] = OutputArtifact(path="/tmp/outputs/artifacts/" + name,
                    archive=sign.archive, save=sign.save)
            else:
                self.outputs.parameters[name] = OutputParameter(value_from_path="/tmp/outputs/parameters/" + name)

        script = ""
        if op_class.__module__ == "__main__":
            source_lines, start_line = inspect.getsourcelines(op_class)
            pre_lines = open(inspect.getsourcefile(op_class), "r").readlines()[:start_line-1]
            script += "".join(pre_lines + source_lines) + "\n"

        script += "import jsonpickle\n"
        script += "from dflow.python import OPIO\n"
        script += "from dflow.python.utils import handle_input_artifact, handle_input_parameter, handle_output\n"
        script += "from %s import %s\n\n" % (op_class.__module__, class_name)
        script += "op_obj = %s()\n" % class_name
        script += "input = OPIO()\n"
        script += "input_sign = %s.get_input_sign()\n" % class_name
        for name, sign in input_sign.items():
            if isinstance(sign, Artifact):
                sign.slices = self.render_slices(sign.slices)
                script += "input['%s'] = handle_input_artifact('%s', input_sign['%s'], %s)\n" % (name, name, name, sign.slices)
            else:
                slices = None
                if input_parameter_slices is not None:
                    slices = self.render_slices(input_parameter_slices.get(name, None))
                script += "input['%s'] = handle_input_parameter('%s', '{{inputs.parameters.%s}}', input_sign['%s'], %s)\n" % (name, name, name, name, slices)

        script += "output = op_obj.execute(input)\n"
        script += "handle_output(output, %s.get_output_sign())\n" % class_name

        self.image = image
        if command is not None:
            self.command = command
        else:
            self.command = ["python"]
        self.script = script
        self.init_progress = "%s/%s" % (op_class.progress_current, op_class.progress_total)
    
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
