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
import os, uuid, time
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
            'task_subdirs': List[str],
            'poscar' : Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        numb_vasp = op_in['numb_vasp']
        olist=[]
        osubdir = []
        for ii in range(numb_vasp):
            ofile = Path(f'task.{ii:04d}')
            osubdir.append(str(ofile))
            ofile.mkdir()
            ofile = ofile/'POSCAR'
            ofile.write_text(f'This is poscar {ii}')
            olist.append(ofile)
        op_out = OPIO({
            'numb_vasp' : numb_vasp,
            "task_subdirs" : osubdir,
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
            'task_subdirs': List[str],
            'potcar' : Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        numb_vasp = op_in['numb_vasp']
        olist=[]
        osubdir = []
        for ii in range(numb_vasp):
            ofile = Path(f'task.{ii:04d}')
            osubdir.append(str(ofile))
            ofile.mkdir()
            ofile = ofile/'POTCAR'
            ofile.write_text(f'This is potcar {ii}')
            olist.append(ofile)
        op_out = OPIO({
            'numb_vasp' : numb_vasp,
            "task_subdirs" : osubdir,
            "potcar": olist,
        })
        return op_out


class RunVasp(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "task_subdir": str,
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
        task_subdir = op_in['task_subdir']
        poscar = op_in['poscar']
        potcar = op_in['potcar']
        # make subdir
        task_subdir = Path(task_subdir)
        task_subdir.mkdir()
        # change to task dir
        cwd = os.getcwd()
        os.chdir(task_subdir)
        # link poscar and potcar 
        if not Path('POSCAR').exists():
            Path('POSCAR').symlink_to(poscar)
        if not Path('POTCAR').exists():
            Path('POTCAR').symlink_to(potcar)
        # write output, assume POSCAR, POTCAR, OUTCAR are in the same dir (task_subdir)
        ofile = Path('OUTCAR')
        ofile.write_text('\n'.join([ Path('POSCAR').read_text() , Path('POTCAR').read_text() ]))
        # chdir
        os.chdir(cwd)
        # output of the OP
        op_out = OPIO({
            "outcar": task_subdir / ofile,
        })
        return op_out


class CollectResult(OP):
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'task_subdirs': List[str],
            'output_common' : Artifact(Path)
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
        task_subdirs = op_in['task_subdirs']
        output_common = op_in['output_common']
        olist = []
        for ii in task_subdirs:
            ofile = output_common / ii / 'OUTCAR'
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

    output_common = S3Artifact(key=str(uuid.uuid4()))
    vasp_run = Step(name="vasp-run",
                    template=PythonOPTemplate(
                        RunVasp,
                        image="dflow:v1.0",
                        input_parameter_slices={
                            "task_subdir": "{{item}}",
                        },
                        input_artifact_slices={
                            "poscar": "{{item}}",
                            "potcar": "{{item}}"
                        },
                        output_artifact_save={
                            "outcar": output_common,
                        },
                        output_artifact_archive={
                            "outcar": None
                        }),
                    parameters = {
                        "task_subdir": make_poscar.outputs.parameters["task_subdirs"],
                    },
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
                        'task_subdirs': make_poscar.outputs.parameters["task_subdirs"],
                    },
                    artifacts={
                        'output_common' : output_common
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
    
