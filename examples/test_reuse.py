from dflow import (
    Workflow,
    Steps,
    Step,
    Inputs,
    InputParameter
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign
)
import time

class Plus1(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'iter' : int
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'iter' : int
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        return OPIO({
            'iter' : op_in['iter'] + 1
        })

if __name__ == "__main__":
    steps = Steps(name="iter", inputs=Inputs(parameters={"iter": InputParameter(value=0), "limit": InputParameter(value=5)}))
    plus1 = Step(name="plus1",
            template=PythonOPTemplate(Plus1,
                    image="python:3.8"),
            parameters={"iter": steps.inputs.parameters["iter"]},
            key="iter-%s" % steps.inputs.parameters["iter"])
    steps.add(plus1)
    next = Step(name="next", template=steps, parameters={"iter": plus1.outputs.parameters["iter"]}, when="%s < %s" % (plus1.outputs.parameters["iter"], steps.inputs.parameters["limit"]))
    steps.add(next)

    wf = Workflow("recurse", steps=steps)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert(wf.query_status() == "Succeeded")

    step0 = wf.query_step(key="iter-0")[0]
    step1 = wf.query_step(key="iter-1")[0]
    step1.modify_output_parameter("iter", 3)

    wf = Workflow("recurse-resubmit", steps=steps)
    wf.submit(reuse_step=[step0, step1])
