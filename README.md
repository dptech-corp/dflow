# DFLOW

Dflow is a Python framework for constructing scientific computing workflows (e.g. concurrent learning workflows) employing [Argo Workflows](https://argoproj.github.io/) as the workflow engine.

For dflow's users (e.g. ML application developers), dflow offers user-friendly functional programming interfaces for building their own workflows. Users need not be concerned with process control, task scheduling, observability and disaster tolerance. Users can track workflow status and handle exceptions by APIs as well as from frontend UI. Thereby users are enabled to concentrate on implementing operators and orchestrating workflows.

For dflow's developers, dflow wraps on argo SDK, keeps details of computing and storage resources from users, and provides extension abilities. While argo is a cloud-native workflow engine, dflow uses containers to decouple computing logic and scheduling logic, and uses Kubernetes to make workflows observable, reproducible and robust. Dflow is designed to be based on a distributed, heterogeneous infrastructure. The most common computing resources in scientific computing may be HPC clusters. Users can either use remote executor to manage HPC jobs within dflow ([dflow-extender](https://github.com/dptech-corp/dflow-extender)), or use operator to uniformly abstract HPC resources in the framework of Kubernetes ([wlm-operator](https://github.com/dptech-corp/wlm-operator)).

<!-- vscode-markdown-toc -->
* 1. [Overview](#Overview)
	* 1.1. [ Architecture](#Architecture)
	* 1.2. [ Common layer](#Commonlayer)
		* 1.2.1. [Parameters and artifacts](#Parametersandartifacts)
		* 1.2.2. [ OP template](#OPtemplate)
		* 1.2.3. [ Workflow](#Workflow)
	* 1.3. [ Interface layer](#Interfacelayer)
		* 1.3.1. [ Python OP](#PythonOP)
* 2. [Quick Start](#QuickStart)
	* 2.1. [Prepare Kubernetes cluster](#PrepareKubernetescluster)
	* 2.2. [Install argo workflows](#Installargoworkflows)
	* 2.3. [Install dflow](#Installdflow)
	* 2.4. [Run an example](#Runanexample)
* 3. [User Guide](#UserGuide)
	* 3.1. [Common layer](#Commonlayer-1)
		* 3.1.1. [Workflow management](#Workflowmanagement)
		* 3.1.2. [Upload/download artifact](#Uploaddownloadartifact)
		* 3.1.3. [Output parameter and artifact of Steps](#OutputparameterandartifactofSteps)
		* 3.1.4. [Conditional step, parameter and artifact](#Conditionalstepparameterandartifact)
		* 3.1.5. [Produce parallel steps using loop](#Produceparallelstepsusingloop)
		* 3.1.6. [Timeout](#Timeout)
		* 3.1.7. [Continue on failed](#Continueonfailed)
		* 3.1.8. [Continue on success number/ratio of parallel steps](#Continueonsuccessnumberratioofparallelsteps)
		* 3.1.9. [Optional input artifact](#Optionalinputartifact)
		* 3.1.10. [Default value for output parameter](#Defaultvalueforoutputparameter)
		* 3.1.11. [Key of step](#Keyofstep)
		* 3.1.12. [Reuse step](#Reusestep)
		* 3.1.13. [Executor](#Executor)
		* 3.1.14. [Submit Slurm job by wlm-operator](#SubmitSlurmjobbywlm-operator)
	* 3.2. [Interface layer](#Interfacelayer-1)
		* 3.2.1. [Slices](#Slices)
		* 3.2.2. [Retry and error handling](#Retryanderrorhandling)
		* 3.2.3. [Progress](#Progress)
		* 3.2.4. [Upload python packages for development](#Uploadpythonpackagesfordevelopment)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->

##  1. <a name='Overview'></a>Overview

###  1.1. <a name='Architecture'></a> Architecture
The dflow consists of a **common layer** and an **interface layer**.  Interface layer takes various OP templates from users, usually in the form of python classes, and transforms them into base OP templates that common layer can handle. Common layer is an extension over argo client which provides functionalities such as file processing, workflow submission and management, etc.

###  1.2. <a name='Commonlayer'></a> Common layer
####  1.2.1. <a name='Parametersandartifacts'></a>Parameters and artifacts
Parameters and artifacts are data stored by the workflow and passed within the workflow. Parameters are saved as strings which can be displayed in the UI, while artifacts are saved as files.

####  1.2.2. <a name='OPtemplate'></a> OP template
OP template describes a sort of operation which takes some parameters and artifacts as input and gives some parameters and artifacts as output. The most common OP template is the container OP template whose operation is specified by container image and commands to be executed as entrypoint. Currently two types of container OP templates are supported. Shell OP template defines an operation by a shell script and Python script OP template defines an operation by a Python script.

- Input parameters: basically a dictionary mapping from parameter name to its properties. The value of the input parameter is optional for the OP template, if provided, it will be regarded as the default value which can be overrided at run time.

- Input artifacts: basically a dictionary mapping from artifact name to its properties. For the container OP template, path where the input artifact is placed in the container is required to be specified.

- Output parameters: basically a dictionary mapping from parameter name to its properties. The source where its value comes from should be specified. For the container OP template, the value may be from a certain file generated in the container (value_from_path).

- Output artifacts: basically a dictionary mapping from artifact name to its properties. For the container OP template, path where the output artifact will be generated in the container is required to be specified.

Here is an example of shell OP template
```python
ShellOPTemplate(name='Hello',
    image="alpine:latest",
    script="cp /tmp/foo.txt /tmp/bar.txt && echo {{inputs.parameters.msg}} > /tmp/msg.txt",
    inputs=Inputs(parameters={"msg": InputParameter()},
        artifacts={"foo": InputArtifact(path="/tmp/foo.txt")}),
    outputs=Outputs(parameters={"msg": OutputParameter(value_from_path="/tmp/result.txt")},
        artifacts={"bar": OutputArtifact(path="/tmp/bar.txt")}))
```

####  1.2.3. <a name='Workflow'></a> Workflow
`Step` and `Steps` are central blocks for building a workflow. A `Step` is the result of instantiating a OP template. When a `Step` is initialized, values of all input parameters and sources of all input artifacts declared in the OP template must be specified. `Steps` is a sequential array of array of concurrent `Step`'s. A simple example goes like `[[s00, s01],  [s10, s11, s12]]`, where inner array represent concurrent tasks while outer array is sequential. A `Workflow` contains a `Steps` as entrypoint for default. Adding a `Step` to a `Workflow` is equivalent to adding the `Step` to the `Steps` of the `Workflow`. For example,
```python
wf = Workflow(name="hhh")
hello0 = Step(name="hello0", template=hello)
wf.add(hello0)
```
Submit a workflow by
```python
wf.submit()
```
- [Steps example](examples/test_steps.py)

It should be noticed that `Steps` itself is a subclass of OPTemplate and could be used as the template of a higher level `Step`. By virtue of this feature, one can construct complex workflows of nested structure. One is also allowed to recursively use a `Steps` as the template of a building block inside itself to achieve dynamic loop.
- [Recursion example](examples/test_recurse.py)

###  1.3. <a name='Interfacelayer'></a> Interface layer
####  1.3.1. <a name='PythonOP'></a> Python OP
Python `OP` is a kind of OP template defined in the form of Python class. As Python is a weak typed language, we impose strict type checking to `OP`s to alleviate ambiguity and unexpected behaviors.

The structures of the inputs and outputs of a `OP` are defined in the static methods `get_input_sign` and `get_output_sign`. Each of them returns a `OPIOSign` object (basically a dictionary mapping from the name of a parameter/artifact to its sign). For a parameter, its sign is its variable type, such as `str`, `float`, `list`, or any user-defined Python class. Since argo only accept string as parameter value, dflow encodes all parameters to json (except for string type parameters) before passing them to argo, and decodes argo parameters from json (except for string type parameters). For an artifact, its sign must be an instance of `Artifact`. `Artifact` receives the type of the path variable as the constructor argument, only `str`, `pathlib.Path`, `typing.Set[str]`, `typing.Set[pathlib.Path]`, `typing.List[str]`, `typing.List[pathlib.Path]` are supported. If a `OP` returns a list of path as an artifact, dflow not only collects files or directories in the returned list of path, and package them in an artifact, but also records their relative path in the artifact. Thus dflow can unpack the artifact to a list of path again before passing to the next `OP`. When no file or directory exists, dflow regards it as `None`.

The execution of the `OP` is defined in the `execute` method. The `execute` method receives a `OPIO` object as input and outputs a `OPIO` object. `OPIO` is basically a dictionary mapping from the name of a parameter/artifact to its value/path. The type of the parameter value or the artifact path should be in accord with that declared in the sign. Type checking is implemented before and after the ` execute` method.

Use `PythonOPTemplate` to convert a `OP` to Python script OP template.
- [Python OP example](examples/test_python.py)

##  2. <a name='QuickStart'></a>Quick Start
###  2.1. <a name='PrepareKubernetescluster'></a>Prepare Kubernetes cluster
Firstly, you'll need a Kubernetes cluster. For quick tests, you can set up a [Minikube](https://minikube.sigs.k8s.io) on your PC.
###  2.2. <a name='Installargoworkflows'></a>Install argo workflows
To get started quickly, you can use the quick start manifest which will install Argo Workflow as well as some commonly used components:
```
kubectl create ns argo
kubectl apply -n argo -f https://raw.githubusercontent.com/dptech-corp/dflow/master/manifests/quick-start-postgres.yaml
```
If you are running Argo Workflows locally (e.g. using Minikube or Docker for Desktop), open a port-forward so you can access the namespace:
```
kubectl -n argo port-forward deployment/argo-server 2746:2746
```
This will serve the user interface on https://localhost:2746
For access to the minio object storage, open a port-forward for minio
```
kubectl -n argo port-forward deployment/minio 9000:9000
```

###  2.3. <a name='Installdflow'></a>Install dflow
Make sure your Python version is not less than 3.7 and install dflow
```
pip install pydflow
```

###  2.4. <a name='Runanexample'></a>Run an example
Submit a simple workflow
```
python examples/test_steps.py
```
Then you can check the submitted workflow through argo's UI.

##  3. <a name='UserGuide'></a>User Guide

###  3.1. <a name='Commonlayer-1'></a>Common layer

####  3.1.1. <a name='Workflowmanagement'></a>Workflow management
After a workflow is submitted by `wf.submit()`, one can track it with APIs

- `wf.id`: workflow ID in argo
- `wf.query_status()`: query workflow status, return `"Pending"`, `"Running"`, `"Suceeded"`, etc.
- `wf.query_step(name=None)`: query step by name (support for regex), return an argo step object
    - `step.phase`: phase of a step, `"Pending"`, `"Running"`, `Succeeded`, etc.
    - `step.outputs.parameters`: a dictionary of output parameters
    - `step.outputs.artifacts`: a dictionary of output artifacts

####  3.1.2. <a name='Uploaddownloadartifact'></a>Upload/download artifact
Dflow offers tools for uploading files to Minio and downloading files from Minio (default object storage in the quick start). User can upload a list of files or directories and get an artifact object, which can be used as argument of a step
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
Note: dflow retains the relative path of the uploaded file/directory with respect to the current directory during uploading. If file/directory outside current directory is uploaded, its absolute path is used as the relative path in the artifact. If you want a different directory structure in the artifact with the local one, you can make soft links and then upload.

####  3.1.3. <a name='OutputparameterandartifactofSteps'></a>Output parameter and artifact of Steps
The output parameter of a `Steps` can be set to come from a step of it by `steps.outputs.parameters["msg"].value_from_parameter = step.outputs.parameters["msg"]`. Here, `step` must be contained in `steps`. For assigning output artifact for a `Steps`, use `steps.outputs.artifacts["foo"]._from = step.outputs.parameters["foo"]`.

####  3.1.4. <a name='Conditionalstepparameterandartifact'></a>Conditional step, parameter and artifact
Set a step to be conditional by `Step(..., when=expr)` where `expr` is an boolean expression in string format. Such as `"%s < %s" % (par1, par2)`. The `when` argument is often used as the breaking condition of recursive steps. The output parameter of a `Steps` can be assigned as optional by
```python
steps.outputs.parameters["msg"].value_from_expression = if_expression(
    _if=par1 < par2,
    _then=par3,
    _else=par4
)
```
Similarly, the output artifact of a `Steps` can be assigned as optional by
```python
steps.outputs.artifacts["foo"].from_expression = if_expression(
    _if=par1 < par2,
    _then=art1,
    _else=art2
)
```

- [Conditional outputs example](examples/test_conditional_outputs.py)

####  3.1.5. <a name='Produceparallelstepsusingloop'></a>Produce parallel steps using loop
`with_param` and `with_sequence` are 2 arguments of `Step` for automatically generating a list of parallel steps. These steps share a common OP template, and only differ in the input parameters.

A step using `with_param` option generates parallel steps on a list (usually from another parameter), the parallelism equals to the length of the list. Each parallel step picks an item from the list by `"{{item}}"`, such as
```python
step = Step(
    ...
    parameters={"msg": "{{item}}"},
    with_param=steps.inputs.parameters["msg_list"]
)
```
A step using `with_sequence` option generates parallel steps on a numeric sequence. `with_sequence` is usually used in coordination with `argo_sequence` (return a sequence, start from 0 for default) and `argo_len` (return length of a list). Each parallel step picks a number from the sequence by `"{{item}}"`, such as
```python
step = Step(
    ...
    parameters={"i": "{{item}}"},
    with_sequence=argo_sequence(argo_len(steps.inputs.parameters["msg_list"]))
)
```

####  3.1.6. <a name='Timeout'></a>Timeout
Set the timeout of a step by `Step(..., timeout=t)`. The unit is second.

- [Timeout example](examples/test_error_handling.py)

####  3.1.7. <a name='Continueonfailed'></a>Continue on failed
Set the workflow to continue when a step fails by `Step(..., continue_on_failed=True)`.

- [Continue on failed example](examples/test_error_handling.py)

####  3.1.8. <a name='Continueonsuccessnumberratioofparallelsteps'></a>Continue on success number/ratio of parallel steps
Set the workflow to continue when certain number/ratio of parallel steps succeed by `Step(..., continue_on_num_success=n)` or `Step(..., continue_on_success_ratio=r)`.

- [Continue on success ratio example](examples/test_success_ratio.py)

####  3.1.9. <a name='Optionalinputartifact'></a>Optional input artifact
Set an input artifact to be optional by `op_template.inputs.artifacts["foo"].optional = True`.

####  3.1.10. <a name='Defaultvalueforoutputparameter'></a>Default value for output parameter
Set default value for a output parameter by `op_template.outputs.parameters["msg"].default = default_value`. The default value will be used when the expression in `value_from_expression` fails or the step is skipped.

####  3.1.11. <a name='Keyofstep'></a>Key of step
You can set a key for a step by `Step(..., key="some-key")` for the convenience of locating the step. The key can be regarded as an input parameter which may contain reference of other parameters. For instance, the key of a step can change with iterations of a dynamic loop. Once key is assigned to a step, the step can be query by `wf.query_step(key="some-key")`. If the key is unique within the workflow, the `query_step` method returns a list consist of only one element.

- [Key of step example](examples/test_reuse.py)

####  3.1.12. <a name='Reusestep'></a>Reuse step
Workflows often have some steps that are expensive to compute. The outputs of previously run steps can be reused for submitting a new workflow. E.g. a failed workflow can be restarted from a certain point after some modification of the workflow template or even outputs of completed steps. For example, submit a workflow with reused steps by `wf.submit(reuse_step=[step0, step1])`. Here, `step0` and `step1` are previously run steps returned by `query_step` method. Before the new workflow runs a step, it will detect if there exists a reused step whose key matches that of the step about to run. If hit, the workflow will skip the step and set its outputs as those of the reused step. To modify outputs of a step before reusing, use `step0.modify_output_parameter(par_name, value)` for parameters and `step0.modify_output_artifact(art_name, artifact)` for artifacts.

- [Reuse step example](examples/test_reuse.py)

####  3.1.13. <a name='Executor'></a>Executor
By default, for a "script step" (a step whose template is a script OP template), the Shell script or Python script runs in the container directly. Alternatively, one can modify the executor to run the script. Dflow offers an extension point for "script step" `Step(..., executor=my_executor)`. Here, `my_executor` should be an instance of class derived from `Executor`. A `Executor`-derived class should specify `image` and `command` to be used for the executor, as well as a method `get_script` which converts original command and script to new script run by the executor.
```python
class Executor(object):
    image = None
    command = None
    def get_script(self, command, script):
        pass
```
`SlurmRemoteExecutor` is provided as an example of executor. The executor submits a slurm job to a remote host and synchronize its status and logs to the dflow step. The central logic of the executor is implemented in the Golang project [Dflow-extender](https://github.com/dptech-corp/dflow-extender). If you want to run a step on a slurm cluster remotely, do something like
```python
Step(
    ...,
    executor=SlurmRemoteExecutor(host="1.2.3.4",
        username="myuser",
        password="mypasswd",
        header="""#!/bin/bash
                  #SBATCH -N 1
                  #SBATCH -n 1
                  #SBATCH -p cpu""")
)
```

####  3.1.14. <a name='SubmitSlurmjobbywlm-operator'></a>Submit Slurm job via virtual node

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

###  3.2. <a name='Interfacelayer-1'></a>Interface layer

####  3.2.1. <a name='Slices'></a>Slices
`Slices` helps user to slice input parameters/artifacts (which must be lists) to feed parallel steps and stack their output parameters/artifacts to lists in the same pattern. For example,
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

- [Slices example](examples/test_slices.py)

####  3.2.2. <a name='Retryanderrorhandling'></a>Retry and error handling
Dflow catches `TransientError` and `FatalError` thrown from `OP`. User can set maximum number of retries on `TransientError` by `PythonOPTemplate(..., retry_on_transient_error=n)`. Timeout error is regarded as fatal error for default. To treat timeout error as transient error, set `PythonOPTemplate(..., timeout_as_transient_error=True)`.

- [Retry example](examples/test_error_handling.py)

####  3.2.3. <a name='Progress'></a>Progress
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

####  3.2.4. <a name='Uploadpythonpackagesfordevelopment'></a>Upload python packages for development
To avoid frequently making image during development, dflow offers a interface to upload local packages into container of `OP` and add them to `$PYTHONPATH`, such as `PythonOPTemplate(python_packages=["/opt/anaconda3/lib/python3.9/site-packages/numpy"])`. One can also globally specify packages to be uploaded, which will affect all `OP`s
```python
from dflow import upload_packages
upload_packages.append("/opt/anaconda3/lib/python3.9/site-packages/numpy")
```
