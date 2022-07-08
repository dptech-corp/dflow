import random

from dflow import Step, Workflow, argo_range
from dflow.python import OP, OPIO, OPIOSign, PythonOPTemplate, TransientError


class Hello(OP):
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
        if random.random() < 0.5:
            raise TransientError("Hello")
        return OPIO()


if __name__ == "__main__":
    wf = Workflow(name="success-ratio")

    step = Step(
        name="hello0",
        template=PythonOPTemplate(Hello, image="python:3.8"),
        continue_on_success_ratio=0.6,
        # continue_on_num_success=3,
        with_param=argo_range(5)
    )
    wf.add(step)
    wf.submit()
