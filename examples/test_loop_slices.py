import time
from typing import List

from dflow import OutputArtifact, Step, Steps, Workflow, argo_range
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
        return OPIOSign({
            'path': List[str]
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        print(op_in["foo"])
        return OPIO({
            'path': op_in["foo"]
        })


def test_loop_slices():
    steps = Steps("slices-steps")
    hello0 = Step("hello0",
                  PythonOPTemplate(Hello, image="python:3.8",
                                   slices=Slices("{{item}}",
                                                 input_parameter=["filename"],
                                                 output_artifact=["foo"]
                                                 )
                                   ),
                  parameters={"filename": ["f1.txt", "f2.txt"]},
                  with_param=argo_range(2),
                  key="hello-0-{{item}}")
    steps.add(hello0)
    check0 = Step("check0",
                  PythonOPTemplate(Check, image="python:3.8"),
                  artifacts={"foo": hello0.outputs.artifacts["foo"]})
    steps.add(check0)
    hello1 = Step("hello1",
                  PythonOPTemplate(Hello, image="python:3.8",
                                   slices=Slices("{{item}}",
                                                 input_parameter=["filename"],
                                                 output_artifact=["foo"]
                                                 )
                                   ),
                  parameters={"filename": []},
                  with_param=argo_range(0),
                  key="hello-1-{{item}}")
    steps.add(hello1)
    check1 = Step("check1",
                  PythonOPTemplate(Check, image="python:3.8"),
                  artifacts={"foo": hello1.outputs.artifacts["foo"]})
    steps.add(check1)
    steps.outputs.artifacts["foo"] = OutputArtifact(
        _from=hello1.outputs.artifacts["foo"])

    wf = Workflow("loop-slices", steps=steps)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")
    step0 = wf.query_step(key="hello-0-0")[0]

    wf2 = Workflow("loop-slices-resubmit", steps=steps)
    wf2.submit(reuse_step=[step0])

    while wf2.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf2.query_status() == "Succeeded")
    check0 = wf2.query_step("check0")[0]
    assert len(check0.outputs.parameters["path"].value) == 2


if __name__ == "__main__":
    test_loop_slices()
