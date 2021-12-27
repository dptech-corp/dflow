from dflow import (
    InputParameter,
    OutputParameter,
    Inputs,
    InputArtifact,
    Outputs,
    OutputArtifact,
    Workflow,
    Step,
    Steps,
    upload_artifact,
    download_artifact,
    S3Artifact,
    argo_range
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact
)
import uuid, time
from typing import Tuple, List
from pathlib import Path

class MakePoscar(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "numb_vasp" : int,
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "numb_vasp" : int,
            'poscar' : Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        numb_vasp = op_in['numb_vasp']
        olist=[]
        for ii in range(numb_vasp):
            ofile = Path(f'task.{ii:04d}')
            ofile.mkdir()
            ofile = ofile/'POSCAR'
            ofile.write_text(f'This is poscar {ii}')
            olist.append(ofile)
        op_out = OPIO({
            'numb_vasp' : numb_vasp,
            "poscar": olist,
        })
        return op_out


class MakePotcar(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "numb_vasp" : int,
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "numb_vasp" : int,
            'potcar' : Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        numb_vasp = op_in['numb_vasp']
        olist=[]
        for ii in range(numb_vasp):
            ofile = Path(f'task.{ii:04d}')
            ofile.mkdir()
            ofile = ofile/'POTCAR'
            ofile.write_text(f'This is potcar {ii}')
            olist.append(ofile)
        op_out = OPIO({
            'numb_vasp' : numb_vasp,
            "potcar": olist,
        })
        return op_out


class RunVasp(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "poscar" : Artifact(Path),
            "potcar" : Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'outcar' : Artifact(Path),
        })
    
    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        poscar = op_in['poscar']
        potcar = op_in['potcar']
        ofile = poscar.parent / 'OUTCAR'
        ofile.write_text('\n'.join([ poscar.read_text() , potcar.read_text() ]))
        op_out = OPIO({
            "outcar": ofile,
        })
        return op_out


class CollectResult(OP):
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'numb_vasp': int,
            'outcar' : Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'outcar' : Artifact(List[Path]),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        numb_vasp = op_in['numb_vasp']
        olist = []
        for ii in range(numb_vasp):
            ofile = op_in['outcar'] / f'task.{ii:04d}' / 'OUTCAR'
            olist.append(ofile)
        return OPIO({
            'outcar': olist
        })


def run_vasp(numb_vasp = 3):    
    vasp_steps = Steps(name="vasp-steps",
                       inputs=Inputs(
                           parameters={
                               "numb_vasp": InputParameter(value=numb_vasp, type=int)
                           })
                       )
    make_poscar = Step(name="make-poscar",
                       template=PythonOPTemplate(MakePoscar,
                                                 image="dflow:v1.0",
                                                 output_artifact_archive={
                                                     "poscar": None
                                                 }),
                       parameters={
                           "numb_vasp": vasp_steps.inputs.parameters['numb_vasp'], 
                       },
                       artifacts={},
                       )    
    make_potcar = Step(name="make-potcar",
                       template=PythonOPTemplate(MakePotcar,
                                                 image="dflow:v1.0",
                                                 output_artifact_archive={
                                                     "potcar": None
                                                 }),
                       parameters={
                           "numb_vasp": vasp_steps.inputs.parameters['numb_vasp'], 
                       },
                       artifacts={},
                       )    
    vasp_steps.add([make_poscar, make_potcar])

    artifact = S3Artifact(key=str(uuid.uuid4()))
    vasp_run = Step(name="vasp-run",
                    template=PythonOPTemplate(
                        RunVasp,
                        image="dflow:v1.0",
                        input_artifact_slices={
                            "poscar": "{{item}}",
                            "potcar": "{{item}}"
                        },
                        output_artifact_save={
                            "outcar": artifact
                        },
                        output_artifact_archive={
                            "outcar": None
                        }),
                    artifacts={
                        "poscar": make_poscar.outputs.artifacts["poscar"],
                        "potcar": make_potcar.outputs.artifacts["potcar"],
                    },
                    with_param=argo_range(vasp_steps.inputs.parameters["numb_vasp"])
                    )
    vasp_steps.add(vasp_run)

    vasp_res = Step(name='vasp-res',
                    template=PythonOPTemplate(CollectResult, image='dflow:v1.0'),
                    parameters={
                        'numb_vasp': vasp_steps.inputs.parameters["numb_vasp"]
                    },
                    artifacts={
                        'outcar' : artifact
                    },
                    )
    vasp_steps.add(vasp_res)

    return vasp_steps
                        

if __name__ == "__main__":
    vasp_steps = run_vasp()
    wf = Workflow(name="vasp", steps=vasp_steps)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(4)

    assert(wf.query_status() == "Succeeded")
    step = wf.query_step(name="vasp-res")[0]
    assert(step.phase == "Succeeded")
    print(download_artifact(step.outputs.artifacts["outcar"]))

    # downloaded artifact: 
    # task.0000/OUTCAR task.0001/OUTCAR task.0002/OUTCAR
    # task.000?/OUTCAR has content
    # This is poscar ?
    # This is potcar ?
    
