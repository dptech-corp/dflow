from clframe import (
    Workflow,
    Step
)
from clframe.python import (
    PythonOPTemplate,
    OP,
    OPParameter,
    OPParameterSign,
    OPArtifact,
    OPArtifactSign
)
from typing import Tuple
import time

class Progress(OP):
    progress_total = 100

    @classmethod
    def get_input_parameter_sign(cls):
        return OPParameterSign()

    @classmethod
    def get_output_parameter_sign(cls):
        return OPParameterSign()
    
    @classmethod
    def get_input_artifact_sign(cls):
        return OPArtifactSign()
    
    @classmethod
    def get_output_artifact_sign(cls):
        return OPArtifactSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_parameter : OPParameter,
            op_artifact : OPArtifact,
    ) -> Tuple[OPParameter, OPArtifact]:
        for i in range(10):
            time.sleep(10)
            self.progress_current = 10 * (i + 1)
        return OPParameter(), OPArtifact()

if __name__ == "__main__":
    wf = Workflow(name="progress")
    step = Step(name="step", template=PythonOPTemplate(Progress, image="clframe:v1.0"))
    # This step will report progress every 10 seconds
    wf.add(step)
    wf.submit()
