# Overview
Dflow is a Python development framework for concurrent learning applications based on [Argo Workflows](https://argoproj.github.io/). Dflow's users are usually ML application developers, e.g. [DPGEN2 project](https://github.com/wanghan-iapcm/dpgen2) is developed in the framework of dflow. Argo is an open-source Kubernetes-native workflow engine. While argo defines workflows via yaml configurations, dflow offers more user-friendly functional programming interfaces.

## Architecture
The dflow consists of a **common layer** and a **interface layer**.  Interface layer takes various OP templates from users, usually in the form of python classes, and transforms them into base OP templates that common layer can handle. Common layer is an extension over argo client which provides functionalities such as file processing, workflow submission and management, etc.

## Common layer
### OP template
OP template describes a sort of operation which takes some parameters and artifacts as input and gives some parameters and artifacts as output. The most common OP template is the container OP template whose operation is specified by container image and commands to be executed in the container. Currently two types of container OP templates are supported. Shell OP template defines an operation by a shell script and Python script OP template defines an operation by a Python script.

- Input parameters: basically a dictionary mapping from parameter name to its properties. The value of the input parameter is optional for the OP template, if provided, it will be regarded as the default value which can be overrided at run time.

- Input artifacts: basically a dictionary mapping from artifact name to its properties. For the container OP template, path where the input artifact is placed in the container is required to be specified.

- Output parameters: basically a dictionary mapping from parameter name to its properties. The source where its value comes from should be specified. For the container OP template, the value may be from a certain file generated in the container (value_from_path).

- Output artifacts: basically a dictionary mapping from artifact name to its properties. For the container OP template, path where the output artifact will be generated in the container is required to be specified.

### Workflow
`Step` and `Steps` are central blocks for building a workflow. A `Step` is the result of instantiating a OP template. When a `Step` is initialized, values of all input parameters and sources of all input artifacts defined in the OP template must be clear. `Steps` is a sequential array of array of concurrent `Step`'s. A simple example goes like `[[s00, s01],  [s10, s11, s12]]`, where inner array represent concurrent tasks while outer array is sequential. A `Workflow` contains a `Steps` as entrypoint for default. Adding a `Step` to a `Workflow` is equivalent to adding the `Step` to the `Steps` of the `Workflow`.

It should be noticed that `Steps` itself is a subclass of OPTemplate and could be used as the template of a higher level `Step`. By virtue of this feature, one can construct complex workflows of nested structure. One is also allowed to recursively use a `Steps` as the template of a building block inside itself to achieve dynamic loop.

## Interface layer
### Python OP
Python `OP` is a kind of OP template defined in the form of Python class. As Python is a weak typed language, we impose strict type checking to `OP`s to alleviate ambiguity and unexpected behaviors.

The structure of the inputs and outputs of a `OP` is defined in the static methods `get_input_sign` and `get_output_sign`, which return a `OPIOSign` object (basically a dictionary mapping from the name of a parameter or an artifact to its sign). For a parameter, its sign is its type, such as str, float, list, etc. For an artifact, its sign must be an instance of `Artifact`. `Artifact` receives the type of the path variable as the constructor argument, which is supposed to be one of `str`, `pathlib.Path`, `typing.Set[str]`, `typing.Set[pathlib.Path]`, `typing.List[str]`, `typing.List[pathlib.Path]`. If a `OP` returns a list of path as an artifact, dflow not only collects files or directories in the returned list of path, and package them in an artifact, but also records their relative path in the artifact. Thus dflow can unpack the artifact to a list of path again before passing to the next `OP`. When no file or directory exists, dflow regards it as `None`.

The execution of the `OP` is defined in the `execute` method. The `execute` method receives a `OPIO` object as input and outputs a `OPIO` object. `OPIO` is basically a dictionary mapping from the name of a parameter/artifact to its value/path. The type of the parameter value or the artifact path should be in accord with that declared in the sign. Type checking is implemented before and after the ` execute` method. Since argo only accept string as parameter value, dflow encodes all parameters to json (except for string type parameters) before passing to argo, and decodes parameters from argo.

Use `PythonOPTemplate` to convert a `OP` to Python script OP template.

# Quick Start
## Prepare Kubernetes cluster
Firstly, you'll need a Kubernetes cluster. For quick tests, you can set up a [Minikube](https://minikube.sigs.k8s.io) on your PC.
## Install argo workflows
To get started quickly, you can use the quick start manifest which will install Argo Workflow as well as some commonly used components:
```
kubectl create ns argo
kubectl apply -n argo -f https://raw.githubusercontent.com/dptech-corp/dflow/master/examples/quick-start-postgres.yaml
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

## Install dflow
Make sure your Python version is not less than 3.7. Download the source code of dflow and install it
```
pip install .
```

## Run an example
Submit a simple workflow
```
python examples/test_steps.py
```
Then you can check the submitted workflow through argo's UI.

# User Guide

## Common layer

### Workflow management
After a workflow is submitted by `wf.submit()`, one can track it with APIs

- `wf.id`: workflow ID in argo
- `wf.query_status()`: query workflow status, return `"Pending"`, `"Running"`, `"Suceeded"`, etc.
- `wf.query_step(name=None)`: query step by name (support for regex), return an argo step object
    - `step.phase`: phase of a step, `"Pending"`, `"Running"`, `Succeeded`, etc.
    - `step.outputs.parameters`: a dictionary of output parameters
    - `step.outputs.artifacts`: a dictionary of output artifacts

### Upload/download artifact
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

### Produce parallel steps using loop
`with_param` and `with_sequence` are 2 optional arguments of `Step` to produce parallel steps.

A step using `with_param` option generates parallel steps on a list (usually from another parameter), the parallelism equals to the length of the list. Each parallel step picks an item from the list by `"{{item}}"`, such as
```python
step = Step(
    ...
    parameter={"msg": "{{item}}"},
    with_param=steps.inputs.parameters["msg_list"]
)
```
A step using `with_sequence` option generates parallel steps on a numeric sequence. `with_sequence` is usually used in coordination with `argo_sequence` (return a sequence, start from 0 for default) and `argo_len` (return length of a list). Each parallel step picks a number from the sequence by `"{{item}}"`, such as
```python
step = Step(
    ...
    parameter={"msg": "{{item}}"},
    with_sequence=argo_sequence(argo_len(steps.inputs.parameters["msg_list"]))
)
```

### Timeout
Set the timeout of a step by `Step(..., timeout=t)`. The unit is second.

### Continue on failed
Set the workflow to continue when a step fails by `Step(..., continue_on_failed=True)`.

### Continue on success number/ratio of parallel steps
Set the workflow to continue when certain number/ratio of parallel steps succeed by `Step(..., continue_on_num_success=n)` or `Step(..., continue_on_success_ratio=r)`.

## Interface layer

### Slices
`Slices` helps user to slice input parameters/artifacts (which must be lists) to feed parallel steps and stack  their output parameters/artifacts to lists in the same pattern. For example,
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

### Retry and error handling
Dflow catches `TransientError` and `FatalError` thrown from `OP`. User can set maximum number of retries on `TransientError` by `PythonOPTemplate(..., retry_on_transient_error=n)`. Timeout error is regarded as fatal error for default. To treat timeout error as transient error, set `PythonOPTemplate(..., timeout_as_transient_error=True)`.