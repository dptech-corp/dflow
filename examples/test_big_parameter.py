import time
from dflow import (
    Workflow,
    Step,
    Steps,
    InputParameter,
    OutputParameter
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    BigParameter
)

class Hello:
    def __init__(self, msg):
        self.msg = msg

class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'foo' : BigParameter(Hello)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'foo' : BigParameter(Hello)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        foo = op_in["foo"]
        print(foo.msg)
        foo.msg = foo.msg * 2
        op_out = OPIO({
            "foo": foo
        })
        return op_out

if __name__ == "__main__":
    wf = Workflow(name="big-param")

    steps = Steps(name="hello-steps")
    steps.inputs.parameters["foo"] = InputParameter()
    steps.outputs.parameters["foo"] = OutputParameter()

    step1 = Step(
        name="step1", 
        template=PythonOPTemplate(Duplicate, image="dptechnology/dflow"),
        parameters={"foo": steps.inputs.parameters["foo"]},
        key="step1"
    )
    steps.add(step1)

    step2 = Step(
        name="step2", 
        template=PythonOPTemplate(Duplicate, image="dptechnology/dflow"),
        parameters={"foo": step1.outputs.parameters["foo"]},
        key="step2"
    )
    steps.add(step2)

    steps.outputs.parameters["foo"].value_from_parameter = step2.outputs.parameters["foo"]

    big_step = Step(name="big-step", template=steps, parameters={"foo": Hello("hello")})
    wf.add(big_step)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")
    step = wf.query_step(name="step1")[0]
    assert(step.phase == "Succeeded")
    print(step.outputs.parameters["foo"].value)

    step.modify_output_parameter("foo", Hello("byebye"))
    wf = Workflow(name="big-param-resubmit")
    wf.add(big_step)
    wf.submit(reuse_step=[step])
