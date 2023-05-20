import time

from dflow import InputParameter, OutputParameter, Step, Steps, Workflow
from dflow.python import (OP, OPIO, BigParameter, OPIOSign, PythonOPTemplate,
                          upload_packages)

if "__file__" in locals():
    upload_packages.append(__file__)


class Hello:
    def __init__(self, msg):
        self.msg = msg


class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'foo': BigParameter(Hello)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'foo': BigParameter(Hello)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        foo = op_in["foo"]
        print(foo.msg)
        foo.msg = foo.msg * 2
        op_out = OPIO({
            "foo": foo
        })
        return op_out


def test_big_parameter():
    wf = Workflow(name="big-param")

    steps = Steps(name="hello-steps")
    steps.inputs.parameters["foo"] = InputParameter()
    steps.outputs.parameters["foo"] = OutputParameter()

    step1 = Step(
        name="step1",
        template=PythonOPTemplate(Duplicate, image="python:3.8"),
        parameters={"foo": steps.inputs.parameters["foo"]},
        key="step1"
    )
    steps.add(step1)

    step2 = Step(
        name="step2",
        template=PythonOPTemplate(Duplicate, image="python:3.8"),
        parameters={"foo": step1.outputs.parameters["foo"]},
        key="step2"
    )
    steps.add(step2)

    steps.outputs.parameters["foo"].value_from_parameter = \
        step2.outputs.parameters["foo"]

    big_step = Step(name="big-step", template=steps,
                    parameters={"foo": Hello("hello")})
    wf.add(big_step)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")
    step = wf.query_step(key="step1")[0]
    assert(step.phase == "Succeeded")
    assert(isinstance(step.outputs.parameters["foo"].value, Hello))


if __name__ == "__main__":
    test_big_parameter()
