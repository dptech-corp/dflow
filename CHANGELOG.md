# CHANGELOG

## [1.2.2](https://github.com/deepmodeling/dflow/compare/v1.2.1...v1.2.2) (2022-07-27)


### Bug Fixes

* call ArgoWorkflow.get_step for multiple times ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))
* change query step by name from fuzzy query to exact query ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))
* **IfExpression:** transfer to right string. ([aee507f](https://github.com/deepmodeling/dflow/commit/aee507f4ea1e7a3c98074edf5d21e5a8db7a7820))
* RFC 1123 of step name ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))
* save each reused step into a single configmap to avoid 1MB limit ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))
* support for query step by a list of conditions ([450cd16](https://github.com/deepmodeling/dflow/commit/450cd16cde9ca31b4701b04ab241f45d7fbc6d85))

## [1.2.1](https://github.com/deepmodeling/dflow/compare/v1.2.0...v1.2.1) (2022-07-24)


### Bug Fixes

* fix a bug of Path(None) ([0920062](https://github.com/deepmodeling/dflow/commit/09200622f236872f023667c899539cef594bd713))

## [1.2.0](https://github.com/deepmodeling/dflow/compare/v1.1.20...v1.2.0) (2022-07-24)


### Features

* add developer guide ([93f161d](https://github.com/deepmodeling/dflow/commit/93f161d520333538f6193d9f1a39235f4dde9862))
* add server-side installation script for Linux ([93f161d](https://github.com/deepmodeling/dflow/commit/93f161d520333538f6193d9f1a39235f4dde9862))
* make ArgoVar uniterable ([93f161d](https://github.com/deepmodeling/dflow/commit/93f161d520333538f6193d9f1a39235f4dde9862))


### Bug Fixes

* cannot pickle types in typing ([691dc57](https://github.com/deepmodeling/dflow/commit/691dc57163844627e89a7e75582141770c194bb4))
* fix a bug of path on Windows ([93f161d](https://github.com/deepmodeling/dflow/commit/93f161d520333538f6193d9f1a39235f4dde9862))
* handle big parameter whose name changes ([7513567](https://github.com/deepmodeling/dflow/commit/75135676452c4d1b028f4e4fd718e2e586f6a461))
* jsonize parameters before reusing ([7ff8272](https://github.com/deepmodeling/dflow/commit/7ff8272424c2a459bf6ba2165b6e49bde5d2bf22))
* line break in input parameter of Python OP ([ae8d11d](https://github.com/deepmodeling/dflow/commit/ae8d11d48d4dc9bbc9e9e815128e9f68eb752bdd))
* **readme:** fix import typo in README ([55f51a0](https://github.com/deepmodeling/dflow/commit/55f51a0c9f97cb7419ac01bd5e787df89aaa9f41))
* reused step does not have outputs ([8abb67d](https://github.com/deepmodeling/dflow/commit/8abb67dfd1df0de2427f1ed9390fde5529641fc5))
* step.outputs has no exitCode ([b98a635](https://github.com/deepmodeling/dflow/commit/b98a63597575f576af1cfd205019ced226c993b9))
* the conflict between input parameter of Python OP and dispatcher script ([c3981fb](https://github.com/deepmodeling/dflow/commit/c3981fbb191bbbedac20a3d8b7361e601c8307b3))

## 1.1.20

- add cloudpickle to dependencies
- add server-side installation script for Mac
- Lebesgue supports for mixed executor

## 1.1.19

- add container mode (docker, singularity or podman) to slurm remote executor, dispatcher executor and wlm executor.

## 1.1.18

- query archived workflows

## 1.1.17

- fit wlm for new features
- add __getitem__ to ArgoVar

## 1.1.16

- support for defining OP in the interactive mode (e.g. jupyter notebook)

## 1.1.15

- a catalog dir instead of catalog files for artifacts
- a better solution for subpath slices

## 1.1.14

- add save_path_as_artifact to handle large path lists

## 1.1.13

- improve uploading python packages
- compatible for python 3.5

## 1.1.12

- fix format problems
- add test examples
- copy_artifact supports for sort

## 1.1.11

### Added

- support for successive sub-path slices steps
- support for path of output artifact referring to path of input artifact in sub-path slices

## 1.1.10

### Added

- update path list of S3Artifact when used

## 1.1.9

### Added

- add type hints
- add interactive mode for Lebesgue's login
- add maximum parallelism for workflows
- add set_directory and run_command to utils

## 1.1.8

### Added

- modify the interface with Lebesgue

## 1.1.7

### Added

- add Parameter type sign for Python OP
- add username and password authentication for Lebesgue executor
- add global config archive_mode

### Fix

- fix: local config does not take effect for remote OP
- fix: dflow_dflow_private_key_path_list was not supplied

## 1.1.6

### Added

- support for DAG
- add catalog_file_name to configuration
- support for passing OP object instead of a derived class of OP to PythonOPTemplate
- add util_command argument to Step

## 1.1.5

### Added

- save path_list as parameter for each artifact
- add sub_path mode for slices

## 1.1.4

### Added

- add some verifications
- prevent mapping relative path repeatedly in dispatcher
- support for a sliced step outputing a list of paths with a single slice
- fix file processing on Windows
- modify interface of resource requests and limits

## 1.1.3

### Fix

- fix: the conflict between input parameter of Python OP and dispatcher script

## 1.1.2

### Fix

- fix: line break in input parameter of Python OP

## 1.1.1

### Added

- support for parallel step and sliced step

## 1.1.0

### Added

- support for using Python OP without dflow in the image

## 1.0.26

### Added

- fix a typo in io
- dicts override json_file in dispatcher executor
- allow for returning None for output artifact in OP
- work around Argo pod id != step id for slices and slurm job template

## 1.0.25

### Added

- support for id_rsa, id_dsa, id_ecdsa, id_ed25519

## 1.0.24

### Added

- fix \n in code for dispatcher executor
- handle io in slurm remote executor

## 1.0.23

### Added

- work around pod id != step id

### Fix

- fix: parameter has no attribute value

## 1.0.22

### Added

- map /tmp to pwd/tmp in slurm job

## 1.0.21

### Added

- add dispatcher executor
- with_param receives python list

## 1.0.20

### Fix

- fix: jsonize parameters before reusing

## 1.0.19

### Fix

- fix: handle big parameter whose name changes

## 1.0.18

### Added

- use str instead of jsonpickle to store parameter types

## 1.0.17

### Fix

- fix: cannot pickle types in typing

## 1.0.16

### Added

- fix a bug in 1.0.15
- record parameter type in parameter description

## 1.0.15

### Added

- handle big parameters in query step and reuse step

## 1.0.14

### Added

- name of OP template be optional
- add not allowed input artifact path
- not use pvc in slurm remote executor for default

## 1.0.13

### Added

- auto build docs to branch docs
- add retry for all actions in slurm remote executor
- support for use private key on node for SSH connection

## 1.0.12

### Added

- force lowercase for OP template to fix RFC 1123
- add private key option for SSH connection
- add container mode for remote Slurm executor
- add docs

## 1.0.11

### Added

- add global configurations
- modify lebesgue interfaces
- request kubernetes API server with token

## 1.0.10

### Added

- add Lebesgue plugins
- add resources requirements for templates
- support for slurm template for slices

## 1.0.9

### Fix

- fix a bug in sliced step

## 1.0.8

### Added

- handle empty sliced step

## 1.0.7

### Added

- add BigParameter
- retain empty dir
- optimize opening file descriptors

## 1.0.6

### Added

- add SlurmJobTemplate to submit slurm job by using wlm-operator

## 1.0.5

### Added

- download_artifact returns path list
- add convert_to_argo method to Workflow

## 1.0.4

### Added

- add resource template
- add some manifests

## 1.0.3

### Added

- add some arguments to PVC

## 1.0.2

### Fix

- restrict argo-workflows==5.0.0

## 1.0.1

### Added

- add comments doc

## 1.0.0

### Added

- initial release
