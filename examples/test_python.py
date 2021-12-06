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
from clframe.python_op import (
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

hello = ContainerOPTemplate(name='Hello',
            image="alpine:latest",
            command=["sh", "-c"],
            args=["echo Hello > /tmp/bar.txt && echo Hello > /tmp/result.txt"])
hello.outputs.parameters = {"msg": OutputParameter(value_from_path="/tmp/result.txt")}
hello.outputs.artifacts = {"bar": OutputArtifact(path="/tmp/bar.txt")}

wf = Workflow(name="hello")
s0 = Step(name="step0", template=hello)
wf.add(s0)
s1 = Step(name="step1", template=PythonOPTemplate(Duplicate, image="clframe:v1.0"), parameters={"msg": s0.outputs.parameters["msg"], "num": 3}, artifacts={"foo": s0.outputs.artifacts["bar"]})
# This step will give output parameter "msg" with value "HelloHelloHello", and output artifact "bar" which contains "Hello\nHello\nHello\n"
wf.add(s1)
wf.submit()
