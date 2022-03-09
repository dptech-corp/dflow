from dflow import (
    Workflow,
    Step,
    upload_artifact,
    download_artifact
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    TransientError,
    FatalError
)
from pathlib import Path
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
    wf = Workflow(name="hello")

    step = Step(
        name="hello0", 
        template=PythonOPTemplate(Hello, image="dptechnology/dflow", retry_on_transient_error=1),
        continue_on_failed=True
    )
    wf.add(step)
    step = Step(
        name="hello1", 
        template=PythonOPTemplate(Timeout, image="dptechnology/dflow", timeout=10, retry_on_transient_error=1, timeout_as_transient_error=True),
        continue_on_failed=True
    )
    wf.add(step)
    step = Step(
        name="hello2", 
        template=PythonOPTemplate(Goodbye, image="dptechnology/dflow", retry_on_transient_error=1)
    )
    wf.add(step)
    wf.submit()
