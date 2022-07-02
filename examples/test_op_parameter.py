from dflow import (
    Workflow,
    Step,
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Parameter,
)

class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'msg' : Parameter(str, default="hello"),
            'num' : Parameter(int, default=5),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'msg' : Parameter(str, default="Hello dflow!"),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        print(op_in["msg"] * op_in["num"])
        return OPIO()

if __name__ == "__main__":
    wf = Workflow(name="op-parameter")

    step = Step(
        name="step", 
        template=PythonOPTemplate(Hello, image="python:3.8")
    )
    wf.add(step)
    wf.submit()
