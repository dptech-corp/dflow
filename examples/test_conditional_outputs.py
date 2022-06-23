from dflow import (
    Workflow,
    Step,
    Steps,
    if_expression,
    Outputs,
    OutputArtifact,
    OutputParameter
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact
)
import random

class Random(OP):
    @classmethod
    def get_input_sign(cls):
        return OPIOSign()

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "is_head": bool,
            "msg1": str,
            "msg2": str,
            "foo": Artifact(str),
            "bar": Artifact(str)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        open("foo.txt", "w").write("head")
        open("bar.txt", "w").write("tail")
        if random.random() < 0.5:
            is_head = True
        else:
            is_head = False
        return OPIO({
            "is_head": is_head,
            "msg1": "head",
            "msg2": "tail",
            "foo": "foo.txt",
            "bar": "bar.txt"
        })

if __name__ == "__main__":
    steps = Steps("conditional-steps", outputs=Outputs(parameters={"msg": OutputParameter()},
            artifacts={"res": OutputArtifact()}))

    random = Step(
        name="random", 
        template=PythonOPTemplate(Random, image="python:3.8")
    )
    steps.add(random)

    steps.outputs.parameters["msg"].value_from_expression = if_expression(
            _if=random.outputs.parameters["is_head"] == True,
            _then=random.outputs.parameters["msg1"], _else=random.outputs.parameters["msg2"])

    steps.outputs.artifacts["res"].from_expression = if_expression(
            _if=random.outputs.parameters["is_head"] == True,
            _then=random.outputs.artifacts["foo"], _else=random.outputs.artifacts["bar"])

    wf = Workflow(name="conditional", steps=steps)

    wf.submit()
