from dflow import (
    InputParameter,
    OutputParameter,
    Inputs,
    InputArtifact,
    OutputArtifact,
    Workflow,
    Step,
    S3Artifact,
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
from typing import Tuple, Set
from pathlib import Path
import os
import time
import uuid

class Generate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign()

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'result' : list,
            'data' : Artifact(Set[Path], archive=None)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        result = []
        data = set()
        for i in range(3):
            result.append({"path": "part-"+str(i)})
            data.add(Path("part-"+str(i)))
            os.makedirs("part-"+str(i), exist_ok=True)
            open("part-"+str(i)+"/msg.txt", "w").write(str(i))

        return OPIO({
            "result": result,
            "data": data
        })

class Process(OP):
    save=None
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'save_path': str,
            'original': Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'processed' : Artifact(Path, archive=None, save=cls.save)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        with open("msg.txt", "w") as f:
            f.write("Hello")
            f.write(open(op_in["original"] / "msg.txt", "r").read())

        return OPIO({
            "processed": Path("msg.txt")
        })

class Collect(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'data': Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'result' : str
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        res = ""
        for d in os.listdir(op_in["data"]):
            with open(os.path.join(op_in["data"], d, "msg.txt"), "r") as f:
                res += f.read()

        return OPIO({
            "result": res
        })

if __name__ == "__main__":
    wf = Workflow(name="parallel")

    prepare_step = Step(
        name="prepare", 
        template=PythonOPTemplate(Generate, image="dflow:v1.0")
    )
    wf.add(prepare_step)

    artifact = S3Artifact(key=str(uuid.uuid4()))
    Process.save = artifact.sub_path("{{inputs.parameters.save_path}}")
    run_step = Step(
        name="run",
        template=PythonOPTemplate(Process, image="dflow:v1.0"),
        parameters={"save_path": "{{item.path}}"},
        artifacts={"original": prepare_step.outputs.artifacts["data"].sub_path("{{item.path}}")},
        with_param=prepare_step.outputs.parameters["result"]
    )
    wf.add(run_step)

    collect_step = Step(
        name="collect",
        template=PythonOPTemplate(Collect, image="dflow:v1.0"),
        artifacts={"data": artifact}
    )
    wf.add(collect_step)

    wf.submit()
