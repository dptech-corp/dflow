import time
from typing import List

from dflow import Step, Workflow, argo_range
from dflow.python import (OP, OPIO, Artifact, OPIOSign, PythonOPTemplate,
                          Slices, upload_packages)

if "__file__" in locals():
    upload_packages.append(__file__)


class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'filename': str
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'foo': Artifact(str)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        open(op_in["filename"], "w").write("foo")
        op_out = OPIO({
            'foo': op_in["filename"]
        })
        return op_out


class Check(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'foo': Artifact(List[str])
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        print(op_in["foo"])
        assert len(op_in["foo"]) == 2
        return OPIO()


def test_slices():
    wf = Workflow("slices")
    hello = Step("hello",
                 PythonOPTemplate(Hello, image="python:3.8",
                                  slices=Slices("{{item}}",
                                                input_parameter=["filename"],
                                                output_artifact=["foo"]
                                                )
                                  ),
                 parameters={"filename": ["f1.txt", "f2.txt"]},
                 with_param=argo_range(2))
    wf.add(hello)
    check = Step("check",
                 PythonOPTemplate(Check, image="python:3.8"),
                 artifacts={"foo": hello.outputs.artifacts["foo"]})
    wf.add(check)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")


if __name__ == "__main__":
    test_slices()
