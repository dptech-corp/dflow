from dflow import (
    InputParameter,
    OutputParameter,
    Inputs,
    InputArtifact,
    OutputArtifact,
    Workflow,
    Step,
    upload_artifact,
    download_artifact
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    ArtifactPath
)
from typing import Tuple, Set
from pathlib import Path
import os
import time

class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'msg' : str,
            'num' : int,
            'foo' : ArtifactPath,
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'msg' : str,
            'bar' : ArtifactPath,
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        content = open(op_in["foo"] / "foo.txt", "r").read()
        open("output.txt", "w").write(content * op_in["num"])
        op_out = OPIO({"msg": op_in["msg"] * op_in["num"], "bar": "output.txt"})
        return op_out

if __name__ == "__main__":
    wf = Workflow(name="hello")
    os.makedirs("inputs", exist_ok=True)
    with open("inputs/foo.txt", "w") as f:
        f.write("Hi")
    artifact = upload_artifact("inputs")
    step = Step(name="step", template=PythonOPTemplate(Duplicate, image="dflow:v1.0"), parameters={"msg": "Hello", "num": 3}, artifacts={"foo": artifact})
    # This step will give output parameter "msg" with value "HelloHelloHello", and output artifact "bar" which contains "HiHiHi"
    wf.add(step)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")
    step = wf.query_step(name="step")[0]
    assert(step.phase == "Succeeded")
    print(download_artifact(step.outputs.artifacts["bar"]))
