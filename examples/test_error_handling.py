from dflow import (
    Workflow,
    Step
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    TransientError,
    FatalError
)
import time

class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign()

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        raise TransientError("Hello")
        return OPIO()

class Timeout(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign()

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        time.sleep(100)
        return OPIO()

class Goodbye(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign()

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        raise FatalError("Goodbye")
        return OPIO()

if __name__ == "__main__":
    wf = Workflow(name="error-handling")

    step = Step(
        name="hello0", 
        template=PythonOPTemplate(Hello, image="python:3.8", retry_on_transient_error=1),
        continue_on_failed=True
    )
    wf.add(step)
    step = Step(
        name="hello1", 
        template=PythonOPTemplate(Timeout, image="python:3.8", timeout=10, retry_on_transient_error=1, timeout_as_transient_error=True),
        continue_on_failed=True
    )
    wf.add(step)
    step = Step(
        name="hello2", 
        template=PythonOPTemplate(Goodbye, image="python:3.8", retry_on_transient_error=1)
    )
    wf.add(step)
    wf.submit()
