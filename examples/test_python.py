from clframe import (
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
    OPIO,
    OPIOSign,
    ArtifactPath
)
from typing import Tuple, Set
from pathlib import Path

class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'msg' : str,
            'num' : int,
            'foo' : Set[ArtifactPath],
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'msg' : str,
            'bar' : Set[ArtifactPath],
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        content = open(list(op_in["foo"])[0], "r").read()
        open("output.txt", "w").write(content * op_in["num"])
        op_out = OPIO({"msg": op_in["msg"] * op_in["num"], "bar": set(["output.txt"])})
        return op_out

if __name__ == "__main__":
    wf = Workflow(name="hello")
    step = Step(name="step", template=PythonOPTemplate(Duplicate, image="clframe:v1.0"), parameters={"msg": "Hello", "num": 3}, artifacts={"foo": "Hi"})
    # This step will give output parameter "msg" with value "HelloHelloHello", and output artifact "bar" which contains "HiHiHi"
    wf.add(step)
    wf.submit()
