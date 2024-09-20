from copy import deepcopy
from typing import Any, List, Optional, Union

from dflow.executor import Executor
from dflow.op_template import ScriptOPTemplate
from dflow.utils import randstr

try:
    from argo.workflows.client import (V1alpha1UserContainer,
                                       V1EmptyDirVolumeSource, V1Volume,
                                       V1VolumeMount)
except ImportError:
    pass


def _ray_init_container(main_image: str,
                        install_mirror: Union[bool, str, None] = None,
                        ray_remote_path_prefix: str = '/tmp/ray',
                        container_name: str = 'ray-init',
                        ):
    """Get init_container with the same image as main_image do. If the
    container have python with ray (import ray), skip the install; else run pip
    install ray.

    Args:
        main_image: image name of main container
        install_mirror: mirror url used by pip install,
                such as tuna: "https://pypi.tuna.tsinghua.edu.cn/simple"
        ray_remote_path_prefix: ray install path (default: "/tmp/ray")

    Returns:
    """
    RAY_INSTALL_STATEMENT = 'pip install ray ' \
                            f'--target={ray_remote_path_prefix}'
    if install_mirror:
        RAY_INSTALL_STATEMENT += f' -i {install_mirror}'
    return V1alpha1UserContainer(
        name=container_name,
        image=main_image,
        command=['sh'],
        args=['-c',
              f"python -c 'import ray' >/dev/null 2>&1 ||"
              f' {{ {RAY_INSTALL_STATEMENT}; }}'],
        volume_mounts=[V1VolumeMount(
            name='ray-python-packages', mount_path=ray_remote_path_prefix)],
    )


class RayClusterExecutor(Executor):

    def __init__(
            self,
            ray_host: str,
            ray_remote_path: str = '/tmp/ray',
            workdir: str = '~/dflow/workflows/{{workflow.name}}/{{pod.name}}',
            ray_install_mirror=None,
            ray_dependencies: Optional[List[Any]] = None,
    ) -> None:
        """Ray cluster executor.

        Args:
            ray_host:
            ray_remote_path:
            workdir:
            ray_install_mirror:
            ray_dependencies: `py_modules` of ray.init(runtime_env={})
        """
        if ray_dependencies is None:
            ray_dependencies = []
        self.ray_host = ray_host
        self.ray_remote_path = ray_remote_path
        self.workdir = workdir
        self.ray_install_mirror = ray_install_mirror
        self.ray_dependencies = ray_dependencies

    def render(self, template: ScriptOPTemplate):
        new_template = deepcopy(template)
        new_template.name += '-' + randstr()
        if new_template.init_containers is None:
            new_template.init_containers = [
                _ray_init_container(template.image, self.ray_install_mirror,
                                    self.ray_remote_path)]
        else:
            assert isinstance(new_template.init_containers,
                              list), f'Type of current init_containers is ' \
                                     f'not list, but ' \
                                     f'{type(new_template.init_containers)}, '\
                                     f'check your other ' \
                                     f'init_container settings.'
            new_template.init_containers.append(
                _ray_init_container(template.image, self.ray_install_mirror,
                                    self.ray_remote_path))
        new_template.script = 'import sys,os                                ' \
                              '\n' \
                              f"sys.path.append('{self.ray_remote_path}')   " \
                              f"\n" \
                              "if os.environ.get('RAY_ADDRESS') is None:    " \
                              "\n" \
                              f"    os.environ['RAY_ADDRESS']=" \
                              f"'{self.ray_host}' " \
                              f"\n" \
                              'else:                                        ' \
                              '\n' \
                              "    print(f\"Not use input ray_host address," \
                              " use {os.environ['RAY_ADDRESS']} instead.\") " \
                              "\n" \
                              + template.script
        # To locate the initialization of package path in `python_op_template`.
        insert_index = new_template.script.find('from dflow import config')
        new_script = list(new_template.script)
        _dependencies_str = ','.join(item.__name__
                                     for item in self.ray_dependencies)
        ray_dependencies_import = \
            f"import {_dependencies_str}\n" if len(
                self.ray_dependencies) > 0 else '\n'
        new_script.insert(
            insert_index,
            'import ray\n' +
            ray_dependencies_import +
            "ray.init(os.environ['RAY_ADDRESS'], runtime_env={"
            f"'py_modules':["
            f"{','.join(item.__name__ for item in self.ray_dependencies)}"
            ']})\n')
        new_template.script = ''.join(new_script)
        new_template.volumes.append(V1Volume(
            name='ray-python-packages', empty_dir=V1EmptyDirVolumeSource()))
        new_template.mounts.append(V1VolumeMount(
            name='ray-python-packages', mount_path=self.ray_remote_path))
        return new_template
