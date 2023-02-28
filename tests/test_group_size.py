import time
from typing import List

from dflow import Step, Workflow, argo_len, argo_range
from dflow.python import (OP, OPIO, Artifact, OPIOSign, PythonOPTemplate,
                          Slices, upload_packages)

if "__file__" in locals():
    upload_packages.append(__file__)


class Prepare(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign()

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'data': List[int]
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        op_out = OPIO({
            'data': list(range(1, 18))
        })
        return op_out


class Process(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'data': int
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "output": Artifact(str)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        with open("%s.txt" % op_in["data"], "w") as f:
            f.write("Hello")
        return OPIO({
            "output": "%s.txt" % op_in["data"]
        })


def test_group_size():
    wf = Workflow("slices")
    prepare = Step("prepare",
                   PythonOPTemplate(Prepare, image="python:3.8"))
    wf.add(prepare)
    process = Step(
        "process",
        PythonOPTemplate(
            Process,
            image="python:3.8",
            slices=Slices(
                "{{item}}",
                input_parameter=["data"],
                output_artifact=["output"],
                pool_size=1,
                group_size=10,
            ),
        ),
        parameters={"data": prepare.outputs.parameters["data"]},
        with_param=argo_range(argo_len(prepare.outputs.parameters["data"])),
    )
    wf.add(process)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")


if __name__ == "__main__":
    test_group_size()
