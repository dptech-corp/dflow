import platform
import time
from pathlib import Path

from dflow import Step, Workflow
from dflow.plugins.ray import RayClusterExecutor
from dflow.python import OP, OPIO, OPIOSign, PythonOPTemplate, upload_packages, Artifact

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
        'result': f"From {node_from} with value {str(input_value)} to {platform.node()}",
        'node_name': platform.node()}


@ray.remote
def count_work_time(one_result):
    if len(one_result)>0:
        result_list = one_result.split()
        from_node = result_list[1]
        to_node = result_list[-1]
        return {
            "from_node": from_node,
            "to_node": to_node,
            "on_node": platform.node()
        }
    else:
        return {
            "on_node": platform.node()
        }



class Sleeps(OP):
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


class CountTheQuotes(OP):
    def __init__(self):
        resources = ray.cluster_resources()
        node_keys = [key for key in resources if 'node' in key]
        num_nodes = sum(resources[node_key] for node_key in node_keys)
        print('num of nodes in cluster:', num_nodes)

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'results': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "count": list
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        results = op_in["results"].read_text().split("\n")
        # Ray need list generators to get ObjectRef and dispatch tasks.
        remote_fun_call = [count_work_time.remote(result_one) for result_one in results]
        op_out = OPIO({
            'count': ray.get(remote_fun_call)
        })
        return op_out


def run_ray():
    wf = Workflow(name='ray-after-ray-test')

    # 1. To run a workflow
    # !!! Please make sure you set the correct ray header url as `ray_host`.
    # !!! The ray_host could be connected INSIDE the kubernetes argoflow on.
    # (often 10.*.*.*:10001)

    # 2. choose an image
    # RayClusterExecutor will exam your image, if it has no ray package with
    # default python (make sure you are not working on some virtual environmental
    # by pip or conda), init container will try to install ray with
    # `pip install ray`.

    # 3. set up mirror if install is needed (optional)
    # For users with special package installation settings,
    # such as private package servers,
    # please set ray_install_mirror to your package server mirror.
    raycluster = RayClusterExecutor(
        ray_host='ray://【】【】【】【】【】【】:10001',  # !!! change this
        ray_install_mirror='https://pypi.tuna.tsinghua.edu.cn/simple'
        # comment this if you don't need
    )

    # !!! change this to same python minor version as your ray cluster.
    IMAGE_NAME = 'python:3.9'

    step1 = Step(
        name='Sleeps',
        template=PythonOPTemplate(Sleeps, image=IMAGE_NAME),
        parameters={'msg': 'Hello', 'num': 10},
        executor=raycluster
    )
    wf.add(step1)
    step2 = Step(
        name='CountTheQuotes',
        template=PythonOPTemplate(CountTheQuotes, image=IMAGE_NAME),
        artifacts={'results': step1.outputs.artifacts["results"]},
        executor=raycluster
    )
    wf.add(step2)
    wf.submit()

if __name__ == '__main__':
    run_ray()
