import os
import time
from pathlib import Path
from typing import List

from dflow import (InputParameter, Inputs, OutputArtifact, Outputs, Step,
                   Steps, Workflow, argo_range, download_artifact)
from dflow.python import (OP, OPIO, Artifact, OPIOSign, PythonOPTemplate,
                          Slices, upload_packages)

if "__file__" in locals():
    upload_packages.append(__file__)


class MakePoscar(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "numb_vasp": int,
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "numb_vasp": int,
            'task_subdirs': List[str],
            'poscar': Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        numb_vasp = op_in['numb_vasp']
        olist = []
        osubdir = []
        for ii in range(numb_vasp):
            print("poscar", ii)
            ofile = Path(f'task.{ii:04d}')
            osubdir.append(str(ofile))
            ofile.mkdir()
            ofile = ofile/'POSCAR'
            ofile.write_text(f'This is poscar {ii}')
            olist.append(ofile)
        op_out = OPIO({
            'numb_vasp': numb_vasp,
            "task_subdirs": osubdir,
            "poscar": olist,
        })
        return op_out


class MakePotcar(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "numb_vasp": int,
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "numb_vasp": int,
            'task_subdirs': List[str],
            'potcar': Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        numb_vasp = op_in['numb_vasp']
        olist = []
        osubdir = []
        for ii in range(numb_vasp):
            print("potcar", ii)
            ofile = Path(f'task.{ii:04d}')
            osubdir.append(str(ofile))
            ofile.mkdir()
            ofile = ofile/'POTCAR'
            ofile.write_text(f'This is potcar {ii}')
            olist.append(ofile)
        op_out = OPIO({
            'numb_vasp': numb_vasp,
            "task_subdirs": osubdir,
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
            "poscar": Artifact(Path),
            "potcar": Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'outcar': Artifact(Path),
            'log': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
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
        # write output, assume POSCAR, POTCAR, OUTCAR are in the same dir
        # (task_subdir)
        ofile = Path('OUTCAR')
        ofile.write_text('\n'.join([Path('POSCAR').read_text(),
                                    Path('POTCAR').read_text()]))
        # write log
        logfile = Path('log')
        logfile.write_text('\n'.join(['this is log', Path(
            'POSCAR').read_text(), Path('POTCAR').read_text()]))
        # chdir
        os.chdir(cwd)
        # output of the OP
        op_out = OPIO({
            "outcar": task_subdir / ofile,
            "log": task_subdir / logfile,
        })
        return op_out


class ShowResult(OP):
    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'outcar': Artifact(List[Path]),
            'log': Artifact(List[Path]),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign()

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        outcar_common = op_in['outcar']
        print(outcar_common)
        log_common = op_in['log']
        print(log_common)
        return OPIO()


def run_vasp(numb_vasp=3):
    vasp_steps = Steps(name="vasp-steps",
                       inputs=Inputs(
                           parameters={
                               "numb_vasp": InputParameter(value=numb_vasp,
                                                           type=int)
                           }),
                       outputs=Outputs(
                           artifacts={
                               "outcar": OutputArtifact(),
                               "log": OutputArtifact(),
                           }),
                       )
    make_poscar = Step(name="make-poscar",
                       template=PythonOPTemplate(MakePoscar,
                                                 image="python:3.8",
                                                 output_artifact_archive={
                                                     "poscar": None
                                                 }),
                       parameters={
                           "numb_vasp": vasp_steps.inputs.parameters[
                            'numb_vasp'],
                       },
                       artifacts={},
                       )
    make_potcar = Step(name="make-potcar",
                       template=PythonOPTemplate(MakePotcar,
                                                 image="python:3.8",
                                                 output_artifact_archive={
                                                     "potcar": None
                                                 }),
                       parameters={
                           "numb_vasp": vasp_steps.inputs.parameters[
                            'numb_vasp'],
                       },
                       artifacts={},
                       )
    vasp_steps.add([make_poscar, make_potcar])

    vasp_run = Step(name="vasp-run",
                    template=PythonOPTemplate(
                        RunVasp,
                        image="python:3.8",
                        slices=Slices("{{item}}",
                                      input_parameter=["task_subdir"],
                                      input_artifact=["poscar", "potcar"],
                                      output_artifact=["outcar", "log"]),
                    ),
                    parameters={
                        "task_subdir": make_poscar.outputs.parameters[
                            "task_subdirs"],
                    },
                    artifacts={
                        "poscar": make_poscar.outputs.artifacts["poscar"],
                        "potcar": make_potcar.outputs.artifacts["potcar"],
                    },
                    with_param=argo_range(
                        vasp_steps.inputs.parameters["numb_vasp"])
                    )
    vasp_steps.add(vasp_run)

    vasp_res = Step(name='vasp-res',
                    template=PythonOPTemplate(ShowResult, image='python:3.8'),
                    artifacts={
                        'outcar': vasp_run.outputs.artifacts["outcar"],
                        'log': vasp_run.outputs.artifacts["log"],
                    },
                    )
    vasp_steps.add(vasp_res)

    vasp_steps.outputs.artifacts['outcar']._from = \
        vasp_run.outputs.artifacts['outcar']
    vasp_steps.outputs.artifacts['log']._from = \
        vasp_run.outputs.artifacts['log']

    return vasp_steps


def test_makevasp():
    vasp_steps = run_vasp()
    wf = Workflow(name='vasp', steps=vasp_steps)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(4)

    assert(wf.query_status() == "Succeeded")
    step = wf.query_step(name="vasp-run")[0]
    assert(step.phase == "Succeeded")
    download_artifact(step.outputs.artifacts["outcar"])
    download_artifact(step.outputs.artifacts["log"])


if __name__ == "__main__":
    test_makevasp()
