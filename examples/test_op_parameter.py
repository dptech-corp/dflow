import time

from dflow import Step, Workflow
from dflow.python import (OP, OPIO, OPIOSign, Parameter, PythonOPTemplate,
                          upload_packages)

if "__file__" in locals():
    upload_packages.append(__file__)


class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'msg': Parameter(str, default="hello"),
            'num': Parameter(int, default=5),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'msg': Parameter(str, default="Hello dflow!"),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        print(op_in["msg"] * op_in["num"])
        return OPIO()


def test_op_parameter():
    wf = Workflow(name="op-parameter")

    step = Step(
        name="step",
        template=PythonOPTemplate(Hello, image="python:3.8")
    )
    wf.add(step)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")


if __name__ == "__main__":
    test_op_parameter()
