import platform
import time
from pathlib import Path

from dflow import Step, Workflow, download_artifact
from dflow.plugins.ray import RayClusterExecutor
from dflow.python import (OP, OPIO, OPIOSign, PythonOPTemplate,
                          upload_packages, Artifact)

if '__file__' in locals():
    upload_packages.append(__file__)

# You need ray to define remote functions
import ray


@ray.remote
def return_value(input_value):
    time.sleep(1)
    return {'input_value': input_value, 'node_name': platform.node()}


@ray.remote
def add_one(inputs):
    input_value = inputs['input_value']
    node_from = inputs["node_name"]
    time.sleep(1)
    return {
        'result': f"From {node_from} "
                  f"with value {str(input_value)} to {platform.node()}",
        'node_name': platform.node()}


class Duplicate(OP):
    def __init__(self):
        resources = ray.cluster_resources()
        node_keys = [key for key in resources if 'node' in key]
        num_nodes = sum(resources[node_key] for node_key in node_keys)
        print('num of nodes in cluster:', num_nodes)

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'msg': str,
            'num': int
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'nodes': Artifact(Path),
            'results': Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        # Ray need list generators to get ObjectRef and dispatch tasks.
        remote_fun_return_true = [add_one.remote(return_value.remote(i)) for i
                                  in
                                  range(op_in['num'])]
        op_out = OPIO({
            'nodes': Path('nodes.txt'),
            'results': Path('results.txt')
        })
        # ray.get() call on remote obj to gather results.
        results = ray.get(remote_fun_return_true)
        for result in results:
            op_out['nodes'].open('a').write(str(result["node_name"]) + "\n")
            op_out['results'].open('a').write(result["result"] + "\n")

        return op_out


def run_ray():
    wf = Workflow(name='ray-test')

    # 1. To run a workflow
    # !!! Please make sure you set the correct ray header url as `ray_host`.
    # !!! The ray_host could be connected INSIDE the kubernetes argoflow on.
    # (often 10.*.*.*:10001)

    # 2. choose an image
    # RayClusterExecutor will exam your image, if it has no ray package with
    # default python (make sure you are not working on some virtual
    # environmental by pip or conda), init container will try to install
    # ray with `pip install ray`.

    # 3. set up mirror if install is needed (optional)
    # For users with special package installation settings,
    # such as private package servers,
    # please set ray_install_mirror to your package server mirror.
    raycluster = RayClusterExecutor(
        ray_host='ray://【】【】【】【】【】:10001',  # !!! change this
        ray_install_mirror='https://pypi.tuna.tsinghua.edu.cn/simple'
        # comment this if you don't need
    )

    # !!! change this to same python minor version as your ray cluster.
    IMAGE_NAME = 'python:3.9'

    step = Step(
        name='step',
        template=PythonOPTemplate(Duplicate, image=IMAGE_NAME),
        parameters={'msg': 'Hello', 'num': 10},
        executor=raycluster
    )
    wf.add(step)
    wf.submit()
    while wf.query_status() in ['Pending', 'Running']:
        time.sleep(1)
    assert (wf.query_status() == 'Succeeded')
    step = wf.query_step(name='step')[0]
    assert (step.phase == 'Succeeded')
    download_artifact(step.outputs.artifacts['nodes'])
    download_artifact(step.outputs.artifacts['results'])


if __name__ == '__main__':
    run_ray()
