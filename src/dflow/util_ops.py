from .io import InputParameter, Inputs
from .op_template import ShellOPTemplate


class CheckNumSuccess(ShellOPTemplate):
    def __init__(self, name="check-num-success", image=None):
        super().__init__(name=name, image=image)
        self.command = ["sh"]
        self.script = "succ=`echo {{inputs.parameters.success}} | grep -o 1 "\
            "| wc -l`\n"
        self.script += "exit $(( $succ < {{inputs.parameters.threshold}} ))"
        self.inputs = Inputs(
            parameters={"success": InputParameter(),
                        "threshold": InputParameter()})


class CheckSuccessRatio(ShellOPTemplate):
    def __init__(self, name="check-success-ratio", image=None):
        super().__init__(name=name, image=image)
        self.command = ["sh"]
        self.script = "succ=`echo {{inputs.parameters.success}} | grep -o 1 |"\
            " wc -l`\n"
        self.script += "fail=`echo {{inputs.parameters.success}} | grep -o 0 "\
            "| wc -l`\n"
        self.script += "exit `echo $succ $fail | awk -v r="\
            "{{inputs.parameters.threshold}} '{if ($1 < ($1+$2)*r)"\
            " {print 1} else {print 0}}'`"
        self.inputs = Inputs(
            parameters={"success": InputParameter(),
                        "threshold": InputParameter()})
