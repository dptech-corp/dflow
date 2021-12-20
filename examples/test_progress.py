from dflow import (
    Workflow,
    Step
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign
)
import time

class Progress(OP):
    progress_total = 100

    @classmethod
    def get_input_sign(cls):
        return OPIOSign()

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        for i in range(10):
            time.sleep(10)
            self.progress_current = 10 * (i + 1)
        return OPIO()

if __name__ == "__main__":
    wf = Workflow(name="progress")
    step = Step(name="step", template=PythonOPTemplate(Progress, image="dflow:v1.0"))
    # This step will report progress every 10 seconds
    wf.add(step)
    wf.submit()
