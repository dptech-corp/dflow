from dflow import (
    Workflow,
    Step,
    upload_artifact
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    Slices
)
from typing import List

class Prepare(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'foo' : Artifact(List[str], archive=None)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        with open("foo1.txt", "w") as f:
            f.write("foo1")
        with open("foo2.txt", "w") as f:
            f.write("foo2")
        op_out = OPIO({
            'foo' : ["foo1.txt", "foo2.txt"]
        })
        return op_out

class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'foo' : Artifact(str)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        with open(op_in["foo"], "r") as f:
            print(f.read())
        return OPIO()

if __name__ == "__main__":
    wf = Workflow("subpath-slices")
    #with open("foo1.txt", "w") as f:
    #    f.write("foo1")
    #with open("foo2.txt", "w") as f:
    #    f.write("foo2")
    #artifact = upload_artifact(["foo1.txt", "foo2.txt"], archive=None)
    prepare = Step("prepare",
            PythonOPTemplate(Prepare, image="python:3.8"))
    wf.add(prepare)

    hello = Step("hello",
            PythonOPTemplate(Hello, image="python:3.8",
                    slices=Slices(sub_path=True,
                        input_artifact=["foo"]
                    )
            ),
            artifacts={"foo": prepare.outputs.artifacts["foo"]})
            #artifacts={"foo": artifact})
    wf.add(hello)
    wf.submit()
