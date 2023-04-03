import sys
from pathlib import Path

from dflow import InputArtifact, InputParameter, OutputArtifact, Step, Steps
from dflow.python import (OP, OPIO, Artifact, OPIOSign, Parameter,
                          PythonOPTemplate)


class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "foo": Artifact(Path, description="input file"),
            "num": Parameter(int, default=2, description="number"),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "bar": Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        with open(op_in["foo"], "r") as f:
            content = f.read()
        with open("bar.txt", "w") as f:
            f.write(content * op_in["num"])
        return OPIO({"bar": Path("bar.txt")})


if __name__ == '__main__':
    from dflow.plugins.launching import OP_to_parser
    steps = Steps(name="duplicate-steps")
    steps.inputs.parameters["num"] = InputParameter(value=2,
                                                    description="number")
    steps.inputs.artifacts["foo"] = InputArtifact(description="input file")
    step = Step(
        name="duplicate",
        template=PythonOPTemplate(Duplicate, image="python:3.8"),
        parameters={"num": steps.inputs.parameters["num"]},
        artifacts={"foo": steps.inputs.artifacts["foo"]})
    steps.add(step)
    steps.outputs.artifacts["bar"] = OutputArtifact(
        _from=step.outputs.artifacts["bar"])
    to_parser = OP_to_parser(steps)
    to_parser()(sys.argv[1:])
