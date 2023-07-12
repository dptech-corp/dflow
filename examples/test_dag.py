from dflow import (InputArtifact, InputParameter, OutputArtifact,
                   OutputParameter, ShellOPTemplate, Task, Workflow)

if __name__ == "__main__":
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
    duplicate.outputs.parameters = {"msg": OutputParameter(
        value_from_path="/tmp/result.txt")}
    duplicate.inputs.artifacts = {"foo": InputArtifact(path="/tmp/foo.txt")}
    duplicate.outputs.artifacts = {"bar": OutputArtifact(path="/tmp/bar.txt")}

    wf = Workflow(name="dag")
    hello0 = Task(name="hello0", template=hello)
    wf.add(hello0)
    hello1 = Task(name="hello1",
                  template=duplicate,
                  parameters={"msg": hello0.outputs.parameters["msg"]},
                  artifacts={"foo": hello0.outputs.artifacts["bar"]})
    wf.add(hello1)
    wf.submit()
