import datetime
from pathlib import Path

from dflow import Step, Workflow, config, upload_artifact  # , S3Artifact
from dflow.plugins.metadata import MetadataClient
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate


class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'foo': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'bar': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        with open(op_in['foo'], "r") as f:
            content = f.read()
        with open("output.txt", "w") as f:
            f.write(content*2)
        self.register_output_artifact(
            "bar",
            namespace="tiefblue",
            dataset_name="dflow-test-%s" % datetime.datetime.now().strftime(
                "%Y%m%d%H%M%S"),
            description="Output artifact",
            tags=["test"],
            properties={"length": "4"},
        )
        return OPIO({
            "bar": Path("output.txt"),
        })


if __name__ == "__main__":
    config["lineage"] = MetadataClient(
        project="<project>",
        token="<token>",
    )
    wf = Workflow(name="metadata")

    with open("foo.txt", "w") as f:
        f.write("Hi")

    # artifact0 = S3Artifact(urn="<urn>")
    artifact0 = upload_artifact(
        "foo.txt",
        namespace="tiefblue",
        dataset_name="dflow-test-%s" % datetime.datetime.now().strftime(
            "%Y%m%d%H%M%S"),
        description="Uploaded artifact",
        tags=["test"],
        properties={"length": "2"})
    print(artifact0.urn)
    step = Step(
        name="step",
        template=PythonOPTemplate(
            Duplicate, image="registry.dp.tech/dptech/dp-metadata-sdk:latest"),
        artifacts={"foo": artifact0},
    )
    wf.add(step)
    wf.submit()
