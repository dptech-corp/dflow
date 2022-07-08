import time

from dflow import (InputParameter, Inputs, OutputParameter, ShellOPTemplate,
                   Step, Steps, Workflow)


def test_recurse():
    plus1 = ShellOPTemplate(
        name='plus1',
        image="alpine:latest",
        script="echo 'This is iter {{inputs.parameters.iter}}' && "
        "echo $(({{inputs.parameters.iter}}+1)) > /tmp/result.txt")
    plus1.inputs.parameters = {"iter": InputParameter()}
    plus1.outputs.parameters = {"iter": OutputParameter(
        value_from_path="/tmp/result.txt")}

    steps = Steps(name="iter", inputs=Inputs(
        parameters={"iter": InputParameter(value=0),
                    "limit": InputParameter(value=3)}))
    hello = Step(name="hello", template=plus1, parameters={
                 "iter": steps.inputs.parameters["iter"]})
    steps.add(hello)
    next = Step(name="next", template=steps,
                parameters={"iter": hello.outputs.parameters["iter"]},
                when="%s < %s" % (
                    hello.outputs.parameters["iter"],
                    steps.inputs.parameters["limit"]))
    # This step use steps as its template (note that Steps is a subclass of
    # OPTemplate), meanwhile the steps it used contains this step,
    # which gives a recursion. The recursion will stop when the "when"
    # condition is satisfied (after 10 loops in this example)
    steps.add(next)

    wf = Workflow("recurse", steps=steps)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")


if __name__ == "__main__":
    test_recurse()
