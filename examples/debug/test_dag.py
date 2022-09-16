from pathlib import Path

from dflow import DAG, Task, Workflow, download_artifact
from dflow.python import (OP, OPIO, Artifact, OPIOSign, PythonOPTemplate,
                          upload_packages)
from dflow import config
config["mode"] = "debug"

if "__file__" in locals():
    upload_packages.append(__file__)


class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign()

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'msg': int,
            'bar': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        with open("output.txt", "w") as f:
            f.write("Hello")

        return OPIO({
            "msg": 1,
            "bar": Path("output.txt"),
        })


class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'msg': int,
            'foo': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'msg': int,
            'bar': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        with open("output.txt", "w") as f:
            with open(op_in["foo"], "r") as f2:
                f.write(f2.read()*2)
        return OPIO({
            "msg": op_in["msg"]*2,
            "bar": Path("output.txt"),
        })


def test_dag():
    dag = DAG()
    hello0 = Task(name="hello0",
                  template=PythonOPTemplate(Hello, image="python:3.8"))
    dag.add(hello0)
    hello1 = Task(name="hello1",
                  template=PythonOPTemplate(Duplicate, image="python:3.8"),
                  parameters={"msg": hello0.outputs.parameters["msg"]},
                  artifacts={"foo": hello0.outputs.artifacts["bar"]})
    dag.add(hello1)

    wf = Workflow(name="dag", dag=dag)
    wf.submit()

    assert hello1.outputs.parameters["msg"].value == 2
    bar = download_artifact(hello1.outputs.artifacts["bar"])[0]
    with open(bar, "r") as f:
        content = f.read()
    assert content == "HelloHello"


if __name__ == "__main__":
    test_dag()
