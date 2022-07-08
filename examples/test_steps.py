import time

from dflow import (InputArtifact, InputParameter, OutputArtifact,
                   OutputParameter, ShellOPTemplate, Step, Workflow,
                   download_artifact)


def test_steps():
    hello = ShellOPTemplate(
        name='Hello',
        image="alpine:latest",
        script="echo Hello > /tmp/bar.txt && echo 1 > /tmp/result.txt")
    hello.outputs.parameters = {"msg": OutputParameter(
        value_from_path="/tmp/result.txt")}
    hello.outputs.artifacts = {"bar": OutputArtifact(path="/tmp/bar.txt")}

    duplicate = ShellOPTemplate(
        name='Duplicate',
        image="alpine:latest",
        script="cat /tmp/foo.txt /tmp/foo.txt > /tmp/bar.txt && "
        "echo $(({{inputs.parameters.msg}}*2)) > /tmp/result.txt")
    duplicate.inputs.parameters = {"msg": InputParameter()}
    duplicate.outputs.parameters = {
        "msg": OutputParameter(value_from_path="/tmp/result.txt")}
    duplicate.inputs.artifacts = {"foo": InputArtifact(path="/tmp/foo.txt")}
    duplicate.outputs.artifacts = {"bar": OutputArtifact(path="/tmp/bar.txt")}

    wf = Workflow(name="steps")
    hello0 = Step(name="hello0", template=hello)
    # This step will give output parameter "msg" with value 1, and output
    # artifact "bar" which contains "Hello"
    wf.add(hello0)
    # This step use the output parameter "msg" of step "hello0" as input
    # parameter "msg", and the output artifact "bar" of step "hello0" as input
    # artifact "foo"
    hello1 = Step(name="hello1", template=duplicate, parameters={
                  "msg": hello0.outputs.parameters["msg"]},
                  artifacts={"foo": hello0.outputs.artifacts["bar"]})
    # This step will give output parameter "msg" with value 2, and output
    # artifact "bar" which contains "HelloHello"
    wf.add(hello1)
    hello2 = Step(name="hello2", template=duplicate, parameters={
                  "msg": hello1.outputs.parameters["msg"]},
                  artifacts={"foo": hello1.outputs.artifacts["bar"]})
    hello3 = Step(name="hello3", template=duplicate, parameters={
                  "msg": hello1.outputs.parameters["msg"]},
                  artifacts={"foo": hello1.outputs.artifacts["bar"]})
    wf.add([hello2, hello3])  # parallel steps
    # These steps will give output parameter "msg" with value 4, and output
    # artifact "bar" which contains "HelloHelloHelloHello"
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")
    step = wf.query_step(name="hello3")[0]
    assert(step.phase == "Succeeded")
    assert(step.outputs.parameters["msg"].value == "4")
    download_artifact(step.outputs.artifacts["bar"])
    assert(open("bar.txt", "r").read() == "Hello\nHello\nHello\nHello\n")


if __name__ == "__main__":
    test_steps()
