from typing import List

from dflow import SlurmJobTemplate, Step, Workflow, argo_range
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices


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
        for filename in op_in["foo"]:
            with open(filename, "r") as f:
                print(f.read())
        return OPIO()


if __name__ == "__main__":
    wf = Workflow(name="wlm")

    hello = Step("hello",
                 PythonOPTemplate(Hello, image="python:3.8",
                                  slices=Slices("{{item}}",
                                                input_parameter=["filename"],
                                                output_artifact=["foo"]
                                                )
                                  ),
                 parameters={"filename": ["f1.txt", "f2.txt"]},
                 with_param=argo_range(2),
                 key="hello-{{item}}",
                 executor=SlurmJobTemplate(
                     header="#!/bin/sh\n#SBATCH --nodes=1",
                     node_selector={
                         "kubernetes.io/hostname": "slurm-minikube-v100"}))
    wf.add(hello)
    check = Step("check",
                 PythonOPTemplate(Check, image="python:3.8"),
                 artifacts={"foo": hello.outputs.artifacts["foo"]})
    wf.add(check)
    wf.submit()
