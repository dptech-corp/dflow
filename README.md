# DFLOW

[Dflow](https://deepmodeling.com/dflow/dflow.html) is a Python framework for constructing scientific computing workflows (e.g. concurrent learning workflows) employing [Argo Workflows](https://argoproj.github.io/) as the workflow engine.

For dflow's users (e.g. ML application developers), dflow offers user-friendly functional programming interfaces for building their own workflows. Users need not be concerned with process control, task scheduling, observability and disaster tolerance. Users can track workflow status and handle exceptions by APIs as well as from frontend UI. Thereby users are enabled to concentrate on implementing operations (OPs) and orchestrating workflows.

For dflow's developers, dflow wraps on argo SDK, keeps details of computing and storage resources from users, and provides extension abilities. While argo is a cloud-native workflow engine, dflow uses containers to decouple computing logic and scheduling logic, and uses Kubernetes to make workflows observable, reproducible and robust. Dflow is designed to be based on a distributed, heterogeneous infrastructure. The most common computing resources in scientific computing may be HPC clusters. User can either use executor to manage HPC jobs using [DPDispatcher](https://github.com/deepmodeling/dpdispatcher) plugin, or use virtual node technique to uniformly manage HPC resources in the framework of Kubernetes ([wlm-operator](https://github.com/dptech-corp/wlm-operator)).

OP template (abbr. OP) in dflow can be reused among workflows and shared among users. Dflow provides a cookie cutter recipe [dflow-op-cutter](https://github.com/deepmodeling/dflow-op-cutter) for template a new OP package. Start developing an OP package at once from
```python
pip install cookiecutter
cookiecutter https://github.com/deepmodeling/dflow-op-cutter.git
```

Dflow provides a debug mode for running workflows bare-metally whose backend is implemented in dflow in pure Python, independent of Argo/Kubernetes. The debug mode uses local environment to execute OPs instead of containers. It implements most APIs of the default mode in order to provide an identical user experience. The debug mode offer convenience for debugging or testing without container. For the clusters having problem deploying docker and Kubernetes and difficult to access from outside, the debug mode may also be used for production, despite less robustness and observability.

<!-- vscode-markdown-toc -->
* 1. [Overview](#1-overview)
	* 1.1. [Architecture](#11-architecture)
	* 1.2. [Basics](#12-basics)
		* 1.2.1. [Parameters and artifacts](#121-parameters-and-artifacts)
		* 1.2.2. [OP template](#122-op-template)
		* 1.2.3. [Step](#123-step)
		* 1.2.4. [Workflow](#124-workflow)
* 2. [Quick Start](#2-quick-start)
	* 2.1. [Setup Argo Server](#21-setup-argo-server)
	* 2.2. [Install dflow](#22-install-dflow)
	* 2.3. [Run an example](#23-run-an-example)
* 3. [User Guide](#3-user-guide)
	* 3.1. [Common layer](#31-common-layer)
		* 3.1.1. [Workflow management](#311-workflow-management)
		* 3.1.2. [Upload/download artifact](#312-upload-download-artifact)
		* 3.1.3. [Steps](#313-steps)
		* 3.1.4. [DAG](#314-dag)
		* 3.1.5. [Conditional step, parameters and artifacts](#315-conditional-step-parameters-and-artifacts)
		* 3.1.6. [Produce parallel steps using loop](#316-produce-parallel-steps-using-loop)
		* 3.1.7. [Timeout](#317-timeout)
		* 3.1.8. [Continue on failed](#318-continue-on-failed)
		* 3.1.9. [Continue on success number/ratio of parallel steps](#319-continue-on-success-number-ratio-of-parallel-steps)
		* 3.1.10. [Optional input artifacts](#3110-optional-input-artifacts)
		* 3.1.11. [Default value for output parameters](#3111-default-value-for-output-parameters)
		* 3.1.12. [Key of a step](#3112-key-of-a-step)
		* 3.1.13. [Resubmit a workflow](#3113-resubmit-a-workflow)
		* 3.1.14. [Executor](#3114-executor)
		* 3.1.15. [Submit HPC/Bohrium job via dispatcher plugin](#3115-submit-hpc-bohrium-job-via-dispatcher-plugin)
		* 3.1.16. [Submit Slurm job via virtual node](#3116-submit-slurm-job-via-virtual-node)
		* 3.1.17. [Use resources in Kubernetes](#3117-use-resources-in-kubernetes)
		* 3.1.18. [Important note: variable names](#3118-important-note-variable-names)
		* 3.1.19. [Debug mode: dflow independent of Kubernetes](#3119-debug-mode-dflow-independent-of-kubernetes)
		* 3.1.20. [Artifact storage plugins](#3120-artifact-storage-plugins)
	* 3.2. [Interface layer](#32-interface-layer)
		* 3.2.1. [Slices](#321-slices)
		* 3.2.2. [Retry and error handling](#322-retry-and-error-handling)
		* 3.2.3. [Progress](#323-progress)
		* 3.2.4. [Upload python packages for development](#324-upload-python-packages-for-development)


<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->

## 1. Overview
###  1.1. Architecture
The dflow consists of a **common layer** and an **interface layer**.  Interface layer takes various OP templates from users, usually in the form of python classes or functions, and transforms them into base OP templates that common layer can handle.

<p align="center">
<img src="./docs/imgs/dflow_architecture.png" alt="dflow_architecture" width="400"/>
</p>
<!-- <style>
img {
  display: block;
  margin-left: auto;
  margin-right: auto;
}
</style> -->

###  1.2. Basics

####  1.2.1.  Parameters and artifacts
Parameters and artifacts are data stored by the workflow and passed within the workflow. Parameters are saved as text which can be displayed in the UI, while artifacts are saved as files. Parameters are passed to an OP with their values, while artifacts are passed as paths.

####  1.2.2.  OP template
OP template (abbr. OP) is a fundamental building block of a workflow. It defines a particular operation to be executed given the input and the expected output. Both the input and output can be parameters and/or artifacts. The most common OP template is the container OP template. Two types of container OP templates are supported: `ShellOPTemplate`, `PythonScriptOPTemplate`. `ShellOPTemplate` defines an operation by shell script and a container image where the script runs. `PythonScriptOPTemplate` defines an operation by Python script and a container image.

As a more Python-native category of OP templates, `PythonOPTemplate` defines OPs in the form of Python classes or Python functions (called class OP or function OP correspondingly). As Python is a weak typed
language, we impose strict type checking to Python OPs to alleviate ambiguity and unexpected behaviors.

For an class OP, the structures of the inputs and outputs of an OP are declared in the static methods `get_input_sign` and `get_output_sign`. Each of them returns a dictionary mapping from the name of a parameter/artifact to its type. The execution of the OP is defined in the `execute` method. The types of the parameter values passed in and out should be in accord with those declared in the sign. Type checking is implemented before and after the `execute` method. For an input/output artifact, its sign should be like `Artifact(type)` where `type` can be `Path`, `List[Path]`, `Dict[str, Path]` or `dflow.python.NestedDict[Path]`. For input artifact, the `execute` method will receive a path, a list of paths or a dictionary of paths according to its sign . OP developer can directly process the file or directory at the path. For output artifact, the `execute` method should also return a path, a list of paths or a dictionary of paths according to its sign.

```python
from dflow.python import OP, OPIO, OPIOSign, Artifact
from pathlib import Path
import shutil


class SimpleExample(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign(
            {
                "msg": str,
                "foo": Artifact(Path),
            }
        )

    @classmethod
    def get_output_sign(cls):
        return OPIOSign(
            {
                "msg": str,
                "bar": Artifact(Path),
            }
        )

    @OP.exec_sign_check
    def execute(
        self,
        op_in: OPIO,
    ) -> OPIO:
        shutil.copy(op_in["foo"], "bar.txt")
        out_msg = op_in["msg"]
        op_out = OPIO(
            {
                "msg": out_msg,
                "bar": Path("bar.txt"),
            }
        )
        return op_out
```

The above example defines an OP `SimpleExample`. The operation is to copy the input artifact `foo` to output artifact `bar` and to copy the input parameter `msg` to output parameter `msg`.

For an function OP, the structure of the inputs and outputs are declared in the type annotations more compactly and execution is defined in the function body. Type checking is implemented before and after the function as well. We recommend `python>=3.9` to use this syntax sugar. See more about Python Annotation at [Python official howtos](https://docs.python.org/3/howto/annotations.html).

```python
from dflow.python import OP, Artifact
from pathlib import Path
import shutil

@OP.function
def SimpleExample(
		msg: str,
		foo: Artifact(Path),
) -> {"msg": str, "bar": Artifact(Path)}:
    shutil.copy(foo, "bar.txt")
    out_msg = msg
    return {"msg": out_msg, "bar": Path("bar.txt")}
```

To define an OP template from the above class or function, we need to specify the container image and other optional arguments to `PythonOPTemplate`. `pydflow` need not to be installed in this image because local `pydflow` package will be uploaded into the container by default

```python
from dflow.python import PythonOPTemplate

simple_example_templ = PythonOPTemplate(SimpleExample, image="python:3.8")
```

An example is here
- [Python OP example](examples/test_python.py)

####  1.2.3. Step
`Step` is the central block for formulating rules of data flows. A step is the result of instantiating an OP template, where values of all input parameters and sources of all input artifacts declared in the OP template must be specified here. The input parameters/artifacts of a step may be either static at the time of submission, or dynamically from outputs of another step.

```python
from dflow import Step

simple_example_step = Step(
    name="step0",
    template=simple_example_templ,
    parameters={"msg": "HelloWorld!"},
    artifacts={"inp_art": foo},
)
``` 

Note that `foo` here is an artifact either uploaded from local or output of another step.


####  1.2.4.  Workflow
`Workflow` connects blocks together to build a workflow. A simple serial workflow is created by adding steps in sequence. Adding a list of steps to a workflow means these steps running in parallel.

```python
from dflow import Workflow

wf = Workflow(name="hello-world")
wf.add(simple_example_step)
```

Submit a workflow by

```python
wf.submit()
```

An example is here
- [Workflow example](examples/test_steps.py)


##  2. Quick Start

###  2.1. Setup Argo Server

If you have an Argo server already, you can skip this step. Otherwise you can follow the [installation guide](tutorials/readme.md).

###  2.2. Install dflow
Make sure your Python version is not less than 3.6 and install dflow

```
pip install pydflow
```

###  2.3. Run an example
There are several [notebook tutorials](tutorials/readme.md) that can help you start to use dflow. Besides, you can submit a simple workflow from the terminal

```
python examples/test_python.py
```

Then you can check the submitted workflow through [argo's UI](https://127.0.0.1:2746).

##  3. User Guide ([dflow-doc](https://deepmodeling.com/dflow/dflow.html))
###  3.1. Common layer

####  3.1.1. Workflow management
After submitting a workflow by `wf.submit()`, or getting a history workflow by `wf = Workflow(id="xxx")`, one can track its real-time status with APIs

- `wf.id`: workflow ID in argo
- `wf.query_status()`: query workflow status, return `"Pending"`, `"Running"`, `"Succeeded"`, etc.
- `wf.query_step(name=None)`: query step by name (support for regex), return a list of argo step objects
    - `step.phase`: phase of a step, `"Pending"`, `"Running"`, `Succeeded`, etc.
    - `step.outputs.parameters`: a dictionary of output parameters
    - `step.outputs.artifacts`: a dictionary of output artifacts

####  3.1.2. Upload/download artifact
Dflow offers tools for uploading files to the artifact repository and downloading files from it (default artifact repository is Minio set up in the quick start). User can upload a file/directory, a list of files/directories or a dictionary of files/directories and get an artifact object, which can be used as argument of a step

```python
artifact = upload_artifact([path1, path2])
step = Step(
    ...
    artifacts={"foo": artifact}
)
```

User can also download the output artifact of a step queried from a workflow (to current directory for default)

```python
step = wf.query_step(name="hello")
download_artifact(step.outputs.artifacts["bar"])
```

Modify `dflow.s3_config` to configure artifact repository settings globally.

Note: dflow retains the relative path of the uploaded file/directory with respect to the current directory during uploading. If file/directory outside current directory is uploaded, its absolute path is used as the relative path in the artifact. If you want a different directory structure in the artifact with the local one, you can make soft links and then upload.

####  3.1.3. Steps
`Steps` is another kind of OP template which is defined by its constituent steps instead of a container. It can be seen as a sub-workflow or a super OP template consisting of some smaller OPs. A `steps` includes an array of arrays of `step`s, e.g. `[[s00,s01],[s10,s11,s12]]`, where inner array represent concurrent steps while outer array is sequential. One can declare input/output parameters/artifacts for a steps by

```python
steps.inputs.parameters["msg"] = InputParameter()
steps.inputs.artifacts["foo"] = InputArtifact()
steps.outputs.parameters["msg"] = OutputParameter()
steps.outputs.parameters["bar"] = OutputArtifact()
```


Add a `step` to a `steps` just like for a `workflow`

```python
steps.add(step)
```

`steps` can be used as the template to instantiate a bigger `step` just like script OP templates. Thus one can construct complex workflows of nested structure. One is also allowed to recursively use a `steps` as the template of a building bloack inside it self to achieve dynamic loop.

The output parameter of a `steps` can be set to come from a step of it by

```python
steps.outputs.parameters["msg"].value_from_parameter = step.outputs.parameters["msg"]
```

Here, `step` must be contained in `steps`. For assigning output artifact for a `steps`, use

```python
steps.outputs.artifacts["foo"]._from = step.outputs.parameters["foo"]
```

- [Recursive example](examples/test_recurse.py)

####  3.1.4. DAG
`DAG` is another kind of OP template which is defined by its constituent tasks and their dependencies. The usage of `DAG` is similar to that of `Steps`. To add a `task` to a `dag`, use

```python
dag.add(task)
```

The usage of `task` is also similar to that of `step`. Dflow will automatically detect dependencies among tasks of a `dag` (from input/output relations). Additional dependencies can be declared by

```python
task_3 = Task(..., dependencies=[task_1, task_2])
```

- [DAG example](examples/test_dag.py)

####  3.1.5. Conditional step, parameters and artifacts
Set a step to be conditional by `Step(..., when=expr)` where `expr` is an boolean expression in string format. Such as `"%s < %s" % (par1, par2)`. The step will be performed if the expression is evalutated to be true, otherwise skipped. The `when` argument is often used as the breaking condition of recursive steps. The output parameter of a `steps` (similar to `dag`) can be assigned as conditional by

```python
steps.outputs.parameters["msg"].value_from_expression = if_expression(
    _if=par1 < par2,
    _then=par3,
    _else=par4
)
```

Similarly, the output artifact of a `steps` can be assigned as conditional by

```python
steps.outputs.artifacts["foo"].from_expression = if_expression(
    _if=par1 < par2,
    _then=art1,
    _else=art2
)
```

- [Conditional outputs example](examples/test_conditional_outputs.py)

####  3.1.6. Produce parallel steps using loop
In scientific computing, it is often required to produce a list of parallel steps which share a common OP template, and only differ in the input parameters. `with_param` and `with_sequence` are 2 arguments of `Step` for automatically generating a list of parallel steps. These steps share a common OP template, and only differ in the input parameters.

A step using `with_param` option generates parallel steps on a list (either a constant list or referring to another parameter, e.g. an output parameter of another step or an input parameter of the `steps` or `DAG` context), the parallelism equals to the length of the list. Each parallel step picks an item from the list by `"{{item}}"`, such as

```python
step = Step(
    ...
    parameters={"msg": "{{item}}"},
    with_param=steps.inputs.parameters["msg_list"]
)
```

A step using `with_sequence` option generates parallel steps on a numeric sequence. `with_sequence` is usually used in coordination with `argo_sequence` which returns an Argo's sequence. For `argo_sequence`, the number at which to start the sequence is specified by `start` (default: 0). One can either specify the number of elements in the sequence by `count` or the number at which to end the sequence by `end`. The printf format string can be specified by `format` to format the value in the sequence. Each argument can be passed with a parameter, `argo_len` which returns the length of a list may be useful. Each parallel step picks an element from the sequence by `"{{item}}"`, such as

```python
step = Step(
    ...
    parameters={"i": "{{item}}"},
    with_sequence=argo_sequence(argo_len(steps.inputs.parameters["msg_list"]))
)
```

####  3.1.7. Timeout
Set the timeout of a step by `Step(..., timeout=t)`. The unit is second.

- [Timeout example](examples/test_error_handling.py)

####  3.1.8. Continue on failed
Set the workflow to continue when a step fails by `Step(..., continue_on_failed=True)`.

- [Continue on failed example](examples/test_error_handling.py)

####  3.1.9. Continue on success number/ratio of parallel steps
For a group of parallel steps generated by `with_param` or `with_sequence`, set the workflow to continue when certain number/ratio of parallel steps succeed by `Step(..., continue_on_num_success=n)` or `Step(..., continue_on_success_ratio=r)`.

- [Continue on success ratio example](examples/test_success_ratio.py)

####  3.1.10. Optional input artifacts
Set an input artifact to be optional by `op_template.inputs.artifacts["foo"].optional = True`.

####  3.1.11. Default value for output parameters
Set default value for an output parameter by `op_template.outputs.parameters["msg"].default = default_value`. The default value will be used when the expression in `value_from_expression` fails or the step is skipped.

####  3.1.12. Key of a step
One can assign a key for a step by `Step(..., key="some-key")` for the convenience of locating the step. The key can be regarded as an input parameter which may contain reference of other parameters. For instance, the key of a step can change with iterations of a dynamic loop. Once key is assigned to a step, the step can be query by `wf.query_step(key="some-key")`. If the key is unique within the workflow, the `query_step` method returns a list consist of only one element.

- [Key of step example](examples/test_reuse.py)

####  3.1.13. Resubmit a workflow
Workflows often have some steps that are expensive to compute. The outputs of previously run steps can be reused for submitting a new workflow. E.g. a failed workflow can be restarted from a certain point after some modification of the workflow template or even outputs of completed steps. For example, submit a workflow with reused steps by `wf.submit(reuse_step=[step0, step1])`. Here, `step0` and `step1` are previously run steps returned by `query_step` method. Before the new workflow runs a step, it will detect if there exists a reused step whose key matches that of the step about to run. If hit, the workflow will skip the step and set its outputs as those of the reused step. To modify outputs of a step before reusing, use `step0.modify_output_parameter(par_name, value)` for parameters and `step0.modify_output_artifact(art_name, artifact)` for artifacts.

- [Reuse step example](examples/test_reuse.py)

####  3.1.14. Executor
For a "script step" (a step whose template is a script OP template), by default the Shell script or Python script runs in the container directly. Alternatively, one can modify the executor to run the script. Dflow offers an extension point for "script step" `Step(..., executor=my_executor)`. Here, `my_executor` should be an instance of class derived from the abstract class `Executor`. An implementation class of `Executor` should implement a method `render` which converts original template to a new template.

```python
class Executor(ABC):
    @abc.abstractmethod
    def render(self, template):
        pass
```

A context is similar to an executor, but assigned to a workflow `Workflow(context=...)` and affect every step.

####  3.1.15. Submit HPC/Bohrium job via dispatcher plugin

[DPDispatcher](https://github.com/deepmodeling/dpdispatcher) is a python package used to generate HPC scheduler systems (Slurm/PBS/LSF) or [Bohrium](https://bohrium.dp.tech) jobs input scripts and submit these scripts and poke until they finish. Dflow provides simple interface to invoke dispatcher as executor to complete script steps. E.g.

```python
from dflow.plugins.dispatcher import DispatcherExecutor
Step(
    ...,
    executor=DispatcherExecutor(host="1.2.3.4",
        username="myuser",
        queue_name="V100")
)
```

For SSH authentication, one can either specify path of private key file locally, or upload authorized private key to each node (or equivalently add each node to the authorized host list). For configuring extra [machine, resources or task parameters for dispatcher](https://docs.deepmodeling.com/projects/dpdispatcher/en/latest/), use `DispatcherExecutor(..., machine_dict=m, resources_dict=r, task_dict=t)`.

- [Dispatcher executor example](examples/test_dispatcher.py)

####  3.1.16. Submit Slurm job via virtual node

Following the installation steps in the [wlm-operator](https://github.com/dptech-corp/wlm-operator) project to add Slurm partitions as virtual nodes to Kubernetes (use manifests [configurator.yaml](manifests/configurator.yaml), [operator-rbac.yaml](manifests/operator-rbac.yaml), [operator.yaml](manifests/operator.yaml) in this project which modified some RBAC configurations)
```
$ kubectl get nodes
NAME                            STATUS   ROLES                  AGE    VERSION
minikube                        Ready    control-plane,master   49d    v1.22.3
slurm-minikube-cpu              Ready    agent                  131m   v1.13.1-vk-N/A
slurm-minikube-dplc-ai-v100x8   Ready    agent                  131m   v1.13.1-vk-N/A
slurm-minikube-v100             Ready    agent                  131m   v1.13.1-vk-N/A
```
Then you can assign a step to be executed on a virtual node (i.e. submit a Slurm job to the corresponding partition to complete the step)

```python
step = Step(
    ...
    executor=SlurmJobTemplate(
        header="#!/bin/sh\n#SBATCH --nodes=1",
        node_selector={"kubernetes.io/hostname": "slurm-minikube-v100"}
    )
)
```

####  3.1.17. Use resources in Kubernetes

A step can also be completed by a Kubernetes resource (e.g. Job or custom resources). At the beginning, a manifest is applied to Kubernetes. Then the status of the resource is monitered until the success condition or the failure condition is satisfied.

```python
class Resource(ABC):
    action = None
    success_condition = None
    failure_condition = None
    @abc.abstractmethod
        pass
```

- [Wlm example](examples/test_wlm.py)

####  3.1.18. Important note: variable names

Dflow has following restrictions on variable names.

| Variable name | Static/Dynamic | Restrictions | Example |
| :------------ | -------------- | ------------ | ------- |
| Workflow/OP template name | Static | Lowercase RFC 1123 subdomain (must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character | my-name |
| Step/Task name | Static | Must consist of alpha-numeric characters or '-', and must start with an alpha-numeric character | My-name1-2, 123-NAME |
| Parameter/Artifact name | Static | Must consist of alpha-numeric characters, '_' or '-' | my_param_1, MY-PARAM-1 |
| Key name | Dynamic | Lowercase RFC 1123 subdomain (must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character | my-name |

####  3.1.19. Debug mode: dflow independent of Kubernetes

The debug mode is enabled by setting

```python
from dflow import config
config["mode"] = "debug"
```

Before running a workflow locally, make sure that the dependencies of all OPs in the workflow are well-configured in the locally environment, unless the dispatcher executor is employed to submit jobs to some remote environments. The debug mode uses the current directory as the working directory by default. Each workflow will create a new directory there, whose structure will be like
```
python-lsev6
├── status
└── step-penf5
    ├── inputs
    │   ├── artifacts
    │   │   ├── dflow_python_packages
    │   │   ├── foo
    │   │   └── idir
    │   └── parameters
    │       ├── msg
    │       └── num
    ├── log.txt
    ├── outputs
    │   ├── artifacts
    │   │   ├── bar
    │   │   └── odir
    │   └── parameters
    │       └── msg
    ├── phase
    ├── script
    ├── type
    └── workdir
        ├── ...
```
The top level contains the status and all steps of the workflow. The directory name for each step will be its key if provided, or generated from its name otherwise. The step directory contains the input/output parameters/artifacts, the type and the phase of the step. For a step of type "Pod", its directory also includes the script, the log file and the working directory for the step.

- [Debug mode examples](examples/debug)

####  3.1.20. Artifact storage plugins

The default storage for artifacts in dflow is a Minio deployment in the Kubernetes cluster. While other artifact storages are supported (e.g. Aliyun OSS, Azure blob storage (ABS), Google cloud storage(GCS)). Dflow provides an extension point to use customized storage in the artifact management. A storage client is a class implementing 5 abstract methods, `upload`, `download`, `list`, `copy` and `get_md5` (optional), which offer the functionality of uploading file, downloading file, listing files with a particular prefix, copying file on the server side and getting the MD5 sum of file, respectively. Use a custom storage client object by configuring s3_config["storage_client"].

```python
class StorageClient(ABC):
    @abc.abstractmethod
    def upload(self, key: str, path: str) -> None:
        pass
    @abc.abstractmethod
    def download(self, key: str, path: str) -> None:
        pass
    @abc.abstractmethod
    def list(self, prefix: str, recursive: bool = False) -> List[str]:
        pass
    @abc.abstractmethod
    def copy(self, src: str, dst: str) -> None:
        pass
    @abc.abstractmethod
    def get_md5(self, key: str) -> str:
        pass
```

###  3.2. Interface layer

####  3.2.1. Slices
In coordination with [parallel steps](#Produceparallelstepsusingloop), `Slices` helps user to slice input parameters/artifacts (which must be lists) to feed parallel steps and stack their output parameters/artifacts to lists in the same pattern. The Python OP only need to handle one slice. For example,

```python
step = Step(name="parallel-tasks",
    template=PythonOPTemplate(
        ...,
        slices=Slices("{{item}}",
            input_parameter=["msg"],
            input_artifact=["data"],
            output_artifact=["log"])
    ),
    parameters = {
        "msg": msg_list
    },
    artifacts={
        "data": data_list
    },
    with_param=argo_range(5)
)
```

In this example, each item in `msg_list` is passed to a parallel step as the input parameter `msg`, each part in `data_list` is passed to a parallel step as the input artifact `data`. Finally, the output artifacts `log` of all parallel steps are collected to one artifact `step.outputs.artifacts["log"]`.
This example is analogous to the following pseudocode
```python
log = [None] * 5
for item in range(5):
    log[item] = my_op(msg=msg_list[item], data=data_list[item])
```
where `with_param` and `slices` corresponds to the `for` loop and the statement in loop in the pseudocode, respectively.

- [Slices example](examples/test_slices.py)

It should be noticed that this feature by default passes full input artifacts to each parallel step which may only use some slices of these artifacts. In comparison, the subpath mode of slices only passes one single slice of the input artifacts to each parallel step. To use the subpath mode of slices,

```python
step = Step(name="parallel-tasks",
    template=PythonOPTemplate(
        ...,
        slices=Slices(sub_path=True,
            input_parameter=["msg"],
            input_artifact=["data"],
            output_artifact=["log"])
    ),
    parameters = {
        "msg": msg_list
    },
    artifacts={
        "data": data_list
    })
```

Here, the slice pattern (`{{item}}`) of `PythonOPTemplate` and the `with_param` argument of the `Step` need not to be set, because they are fixed in this mode. Each input parameter and artifact to be sliced must be of the same length, and the parallelism equals to this length. Another noticeable point is that in order to use the subpath of the artifacts, these artifacts must be saved without compression when they are generated. E.g. declare `Artifact(..., archive=None)` in the output signs of Python OP, or specify `upload_artifact(..., archive=None)` while uploading artifacts. Besides, one can use `dflow.config["archive_mode"] = None` to set default archive mode to no compression globally.

- [Subpath mode of slices example](examples/test_subpath_slices.py)

####  3.2.2. Retry and error handling
Dflow catches `TransientError` and `FatalError` thrown from `OP`. User can set maximum number of retries on `TransientError` by `PythonOPTemplate(..., retry_on_transient_error=n)`. Timeout error is regarded as fatal error for default. To treat timeout error as transient error, set `PythonOPTemplate(..., timeout_as_transient_error=True)`. When a fatal error is raised or the retries on transient error reaches maximum retries, the step is considered as failed.

- [Retry example](examples/test_error_handling.py)

####  3.2.3. Progress
A `OP` can update progress in the runtime so that user can track its real-time progress

```python
class Progress(OP):
    progress_total = 100
    ...
    def execute(op_in):
        for i in range(10):
            self.progress_current = 10 * (i + 1)
            ...
```

- [Progress example](examples/test_progress.py)

####  3.2.4. Upload python packages for development
To avoid frequently making image during development, dflow offers an interface to upload local packages into container and add them to `$PYTHONPATH`, such as `PythonOPTemplate(..., python_packages=["/opt/anaconda3/lib/python3.9/site-packages/numpy"])`. One can also globally specify packages to be uploaded, which will affect all `OP`s

```python
from dflow.python import upload_packages
upload_packages.append("/opt/anaconda3/lib/python3.9/site-packages/numpy")
```
