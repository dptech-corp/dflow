from clframe import (
    ContainerOPTemplate,
    InputParameter,
    OutputParameter,
    Inputs,
    InputArtifact,
    OutputArtifact,
    Workflow,
    Step
)
from clframe.python import (
    PythonOPTemplate,
    OP,
    OPParameter,
    OPParameterSign,
    OPArtifact,
    OPArtifactSign
)
from typing import Tuple, Set
from pathlib import Path

class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_parameter_sign(cls):
        return OPParameterSign({
            'msg' : str,
            'num' : int
        })

    @classmethod
    def get_output_parameter_sign(cls):
        return OPParameterSign({
            'msg' : str,
        })
    
    @classmethod
    def get_input_artifact_sign(cls):
        return OPArtifactSign({
            'foo' : Path
        })
    
    @classmethod
    def get_output_artifact_sign(cls):
        return OPArtifactSign({
            'bar' : Path
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_parameter : OPParameter,
            op_artifact : OPArtifact,
    ) -> Tuple[OPParameter, OPArtifact]:
        ret_parameter = OPParameter({"msg": op_parameter["msg"] * op_parameter["num"]})
        content = open(op_artifact["foo"], "r").read()
        open("output.txt", "w").write(content * op_parameter["num"])
        ret_artifact = OPArtifact({"bar": "output.txt"})
        return ret_parameter, ret_artifact

if __name__ == "__main__":
    wf = Workflow(name="hello")
    step = Step(name="step", template=PythonOPTemplate(Duplicate, image="clframe:v1.0"), parameters={"msg": "Hello", "num": 3}, artifacts={"foo": "Hi"})
    # This step will give output parameter "msg" with value "HelloHelloHello", and output artifact "bar" which contains "HiHiHi"
    wf.add(step)
    wf.submit()
