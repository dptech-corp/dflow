from dflow import (
    Workflow,
    Step,
    upload_artifact,
    download_artifact
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact
)
from dflow.plugins.dispatcher import DispatcherExecutor
from typing import List
from pathlib import Path
import time

class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'msg' : str,
            'num' : int,
            'foo' : Artifact(Path),
            'idir' : Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'msg' : List[str],
            'bar' : Artifact(Path),
            'odir' : Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        op_out = OPIO({
            "msg": [op_in["msg"] * op_in["num"]], 
            "bar": Path("output.txt"),
            "odir" : Path("todir"),
        })

        content = open(op_in['foo'], "r").read()
        open("output.txt", "w").write(content * op_in["num"])

        Path(op_out['odir']).mkdir()
        for ii in ['f1', 'f2']:
            (op_out['odir']/ii).write_text(op_in['num'] * (op_in['idir']/ii).read_text())

        return op_out


def make_idir():
    idir = Path("tidir")
    idir.mkdir(exist_ok=True)
    (idir / "f1").write_text("foo")
    (idir / "f2").write_text("bar")


if __name__ == "__main__":
    wf = Workflow(name="hello")

    with open("foo.txt", "w") as f:
        f.write("Hi")
    make_idir()

    artifact0 = upload_artifact("foo.txt")
    artifact1 = upload_artifact("tidir")
    print(artifact0)
    print(artifact1)
    dispatcher_executor = DispatcherExecutor(host="my-host", username="my-user", queue_name="V100")
    step = Step(
        name="step", 
        template=PythonOPTemplate(Duplicate, image="dptechnology/dflow", image_pull_policy="IfNotPresent"),
        parameters={"msg": "Hello", "num": 3}, 
        artifacts={"foo": artifact0, "idir": artifact1},
        executor=dispatcher_executor
    )
    # This step will give output parameter "msg" with value "HelloHelloHello", and output artifact "bar" which contains "HiHiHi"
    # output artifact "odir/f1" contains foofoofoo and "odir/f2" contains barbarbar
    wf.add(step)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")
    step = wf.query_step(name="step")[0]
    assert(step.phase == "Succeeded")

    print(download_artifact(step.outputs.artifacts["bar"]))
    print(download_artifact(step.outputs.artifacts["odir"]))

    print(step.outputs.parameters["msg"].value, type(step.outputs.parameters["msg"].value))
