import time
from pathlib import Path

from dflow import Step, Workflow, download_artifact, upload_artifact
from dflow.python import OP, Artifact, PythonOPTemplate, upload_packages

if '__file__' in locals():
    upload_packages.append(__file__)


@OP.function
def Duplicate(msg: str, num: int, foo: Artifact(Path), idir: Artifact(Path)
              ) -> {'msg': str, 'bar': Artifact(Path), 'odir': Artifact(Path)}:
    msg_out = msg * num
    bar = Path('output.txt')
    odir = Path('todir')
    content = open(foo, 'r').read()
    open('output.txt', 'w').write(content * num)
    odir.mkdir()
    for ii in ['f1', 'f2']:
        (odir / ii).write_text(num * (idir / ii).read_text())
    return {'msg': msg_out, 'bar': Path(bar), 'odir': Path(odir), }


def make_idir():
    idir = Path('tidir')
    idir.mkdir(exist_ok=True)
    (idir / 'f1').write_text('foo')
    (idir / 'f2').write_text('bar')


def test_python():
    with open('foo.txt', 'w') as f:
        f.write('Hi')
    make_idir()

    artifact0 = upload_artifact('foo.txt')
    artifact1 = upload_artifact('tidir')
    print(artifact0)
    print(artifact1)

    with Workflow(name='python-sugar') as wf:
        step = Step(
            name='step', template=PythonOPTemplate(
                Duplicate, image='python:3.8'), parameters={
                'msg': 'Hello', 'num': 3}, artifacts={
                'foo': artifact0, 'idir': artifact1}, )

    while wf.query_status() in ['Pending', 'Running']:
        time.sleep(1)

    assert (wf.query_status() == 'Succeeded')
    step = wf.query_step(name='step')[0]
    assert (step.phase == 'Succeeded')

    print(download_artifact(step.outputs.artifacts['bar']))
    print(download_artifact(step.outputs.artifacts['odir']))


if __name__ == '__main__':
    test_python()
