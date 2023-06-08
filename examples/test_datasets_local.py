import os
from pathlib import Path

from dflow import Step, Workflow
from dflow.plugins.datasets import DatasetsArtifact, config
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate


class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'foo': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        print(os.listdir(op_in["foo"]))
        return OPIO()


if __name__ == "__main__":
    wf = Workflow(name="datasets")
    # art = DatasetsArtifact.from_rclone_config("""
    # [pdbbind@0.1.2]
    # type = ftp
    # disable_tls13 = true
    # concurrency = 3
    # host = uftp.mlops-passthrough.dp.tech
    # user = xxx
    # pass = xxx
    # explicit_tls = true""")
    config["user"] = "xxx"
    config["password"] = "xxx"
    art = DatasetsArtifact.from_urn("launching+datasets://pdbbind@0.1.2")
    step = Step(
        name="step",
        template=PythonOPTemplate(Hello, image="python:3.8"),
        artifacts={"foo": art},
    )
    wf.add(step)
    wf.submit()
