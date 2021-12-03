from clframe import (
    ContainerOPTemplate,
    InputParameter,
    OutputParameter,
    InputArtifact,
    OutputArtifact,
    Workflow,
    Step
)

hello = ContainerOPTemplate(name='Hello',
            image="alpine:latest",
            command=["sh", "-c"],
            args=["echo Hello > /tmp/bar.txt && echo 1 > /tmp/result.txt"])
hello.outputs.parameters = {"msg": OutputParameter(value_from_path="/tmp/result.txt")}
hello.outputs.artifacts = {"bar": OutputArtifact(path="/tmp/bar.txt")}

duplicate = ContainerOPTemplate(name='Duplicate',
            image="alpine:latest",
            command=["sh", "-c"],
            args=["cat /tmp/foo.txt /tmp/foo.txt > /tmp/bar.txt && echo $(({{inputs.parameters.msg}}*2)) > /tmp/result.txt"])
duplicate.inputs.parameters = {"msg": InputParameter()}
duplicate.outputs.parameters = {"msg": OutputParameter(value_from_path="/tmp/result.txt")}
duplicate.inputs.artifacts = {"foo": InputArtifact(path="/tmp/foo.txt")}
duplicate.outputs.artifacts = {"bar": OutputArtifact(path="/tmp/bar.txt")}

wf = Workflow(name="hhh")
hello0 = Step(name="hello0", template=hello)
wf.add(hello0)
hello1 = Step(name="hello1", template=duplicate, parameters={"msg": hello0.outputs.parameters["msg"]}, artifacts={"foo": hello0.outputs.artifacts["bar"]})
wf.add(hello1)
hello2 = Step(name="hello2", template=duplicate, parameters={"msg": hello1.outputs.parameters["msg"]}, artifacts={"foo": hello1.outputs.artifacts["bar"]})
hello3 = Step(name="hello3", template=duplicate, parameters={"msg": hello1.outputs.parameters["msg"]}, artifacts={"foo": hello1.outputs.artifacts["bar"]})
wf.add([hello2, hello3]) # parallel steps
wf.submit()
