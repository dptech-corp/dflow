from dflow import (
    Workflow,
    Step,
    SlurmJob
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign
)

class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        print("Hello")
        return OPIO({
        })

if __name__ == "__main__":
    wf = Workflow(name="hello")
    step = Step(
        name="hello",
        template=PythonOPTemplate(Hello, image="dptechnology/dflow"),
        use_resource=SlurmJob(header="#!/bin/sh\n#SBATCH --nodes=1")
    )
    wf.add(step)
    wf.submit()
