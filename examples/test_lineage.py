from pathlib import Path

from dflow import Step, Workflow, config, upload_artifact
from dflow.plugins.metadata import MetadataClient
from dflow.python import OP, Artifact, PythonOPTemplate


@OP.function
def duplicate(foo: Artifact(Path)) -> {'bar': Artifact(Path)}:
    with open(foo, 'r') as f:
        content = f.read()
    with open('output.txt', 'w') as f:
        f.write(content * 2)
    return {'bar': Path('output.txt')}


if __name__ == "__main__":
    config["lineage"] = MetadataClient(
        gms_endpoint="https://datahub-test-gms.codespace.dp.tech",
        token="<token>",
    )

    wf = Workflow(name="lineage")

    with open("foo.txt", "w") as f:
        f.write("Hi")

    art = upload_artifact("foo.txt", namespace="lineage-test", name="foo")
    step1 = Step(
        name="step1",
        template=PythonOPTemplate(duplicate,
                                  image="dptechnology/datahub-metadata-sdk"),
        artifacts={"foo": art},
    )
    wf.add(step1)

    step2 = Step(
        name="step2",
        template=PythonOPTemplate(duplicate,
                                  image="dptechnology/datahub-metadata-sdk"),
        artifacts={"foo": step1.outputs.artifacts["bar"]},
    )
    wf.add(step2)
    wf.submit()
