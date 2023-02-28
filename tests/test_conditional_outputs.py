import time

from dflow import (OutputArtifact, OutputParameter, Outputs, Step, Steps,
                   Workflow, if_expression)
from dflow.python import (OP, OPIO, Artifact, OPIOSign, PythonOPTemplate,
                          upload_packages)

if "__file__" in locals():
    upload_packages.append(__file__)


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
            op_in: OPIO,
    ) -> OPIO:
        open("foo.txt", "w").write("head")
        open("bar.txt", "w").write("tail")
        is_head = False
        return OPIO({
            "is_head": is_head,
            "msg1": "head",
            "msg2": "tail",
            "foo": "foo.txt",
            "bar": "bar.txt"
        })


def test_conditional_outputs():
    steps = Steps("steps", outputs=Outputs(
        parameters={"msg": OutputParameter()},
        artifacts={"res": OutputArtifact()}))

    random_step = Step(
        name="random",
        template=PythonOPTemplate(Random, image="python:3.8")
    )
    steps.add(random_step)

    steps.outputs.parameters["msg"].value_from_expression = if_expression(
        _if=random_step.outputs.parameters["is_head"],
        _then=random_step.outputs.parameters["msg1"],
        _else=random_step.outputs.parameters["msg2"])

    steps.outputs.artifacts["res"].from_expression = if_expression(
        _if=random_step.outputs.parameters["is_head"],
        _then=random_step.outputs.artifacts["foo"],
        _else=random_step.outputs.artifacts["bar"])

    wf = Workflow(name="conditional")
    step = Step("conditional", template=steps)
    wf.add(step)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")
    step = wf.query_step(name="conditional")[0]
    assert(step.phase == "Succeeded")
    assert step.outputs.parameters["msg"].value == "tail"


if __name__ == "__main__":
    test_conditional_outputs()
