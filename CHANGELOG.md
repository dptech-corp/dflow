# CHANGELOG

## [1.5.10](https://github.com/deepmodeling/dflow/compare/v1.5.9...v1.5.10) (2022-10-03)


### Bug Fixes

* add reused keys to global outputs ([5ea6b53](https://github.com/deepmodeling/dflow/commit/5ea6b536e8e8326d3b1e7f726e71671602e2c9bf))
* print uid when submitting ([5ea6b53](https://github.com/deepmodeling/dflow/commit/5ea6b536e8e8326d3b1e7f726e71671602e2c9bf))

## [1.5.9](https://github.com/deepmodeling/dflow/compare/v1.5.8...v1.5.9) (2022-10-03)


### Bug Fixes

* add requests to dependencies ([ac2c7f4](https://github.com/deepmodeling/dflow/commit/ac2c7f4a945381f1bc62f2acf555a134aa10503c))
* bug of parallelism for sliced step with sequence ([0729d1e](https://github.com/deepmodeling/dflow/commit/0729d1e398baa9d82dd741b026f5c23e431a292d))

## [1.5.8](https://github.com/deepmodeling/dflow/compare/v1.5.7...v1.5.8) (2022-09-28)


### Bug Fixes

* raise FileNotFoundError when downloading an artifact without key ([615a934](https://github.com/deepmodeling/dflow/commit/615a934b6c40ee5400aa8cc810c30b72dbcde85e))
* replace python command to python3 ([9c6772a](https://github.com/deepmodeling/dflow/commit/9c6772a49ba86730c6db125d25547551971e64fe))
* save uid after submission ([c696f1d](https://github.com/deepmodeling/dflow/commit/c696f1dadd52691242c2ac8668c573bbb16358c4))
* some bugs of dispatcher executor ([9c6772a](https://github.com/deepmodeling/dflow/commit/9c6772a49ba86730c6db125d25547551971e64fe))
* support for local mode of dispatcher ([9c6772a](https://github.com/deepmodeling/dflow/commit/9c6772a49ba86730c6db125d25547551971e64fe))

## [1.5.7](https://github.com/deepmodeling/dflow/compare/v1.5.6...v1.5.7) (2022-09-24)


### Bug Fixes

* add image pull secrets for workflow ([df69681](https://github.com/deepmodeling/dflow/commit/df696817c9df3063a7a1740eefe20dcdde9a2bf4))
* refact FutureLen, add FutureRange ([99730e2](https://github.com/deepmodeling/dflow/commit/99730e2865236c9b6b46682fe629b10834e8d8ae))
* support for skipping same files while downloading ([df69681](https://github.com/deepmodeling/dflow/commit/df696817c9df3063a7a1740eefe20dcdde9a2bf4))

## [1.5.6](https://github.com/deepmodeling/dflow/compare/v1.5.5...v1.5.6) (2022-09-22)


### Bug Fixes

* add methods for OP to get s3 key/link of input/output artifacts ([8a12911](https://github.com/deepmodeling/dflow/commit/8a12911bbc4f451ef0d70345346acbe09430abf2))
* add slurm docker cluster for autotests ([ef60d79](https://github.com/deepmodeling/dflow/commit/ef60d79690db125a94f7c852686b8acb35ae4c0d))

## [1.5.5](https://github.com/deepmodeling/dflow/compare/v1.5.4...v1.5.5) (2022-09-20)


### Bug Fixes

* pass steps/tasks in the initialization of Steps/DAG ([9ef5fd7](https://github.com/deepmodeling/dflow/commit/9ef5fd7a1d1c2c4fd52cc6952d5655020d7760e1))
* prepare_step is None ([9ef5fd7](https://github.com/deepmodeling/dflow/commit/9ef5fd7a1d1c2c4fd52cc6952d5655020d7760e1))

## [1.5.4](https://github.com/deepmodeling/dflow/compare/v1.5.3...v1.5.4) (2022-09-19)


### Bug Fixes

* support parallelism for a parallel step other than sliced step ([d62de45](https://github.com/deepmodeling/dflow/commit/d62de45cc8d21fbd0143fc156fad187ac0bcdf2e))

## [1.5.3](https://github.com/deepmodeling/dflow/compare/v1.5.2...v1.5.3) (2022-09-19)


### Bug Fixes

* support for any private key file name ([ded49c9](https://github.com/deepmodeling/dflow/commit/ded49c9ef06a946d9082680dbced795c07f8ab07))

## [1.5.2](https://github.com/deepmodeling/dflow/compare/v1.5.1...v1.5.2) (2022-09-18)


### Bug Fixes

* add parallelism for sliced step ([e620285](https://github.com/deepmodeling/dflow/commit/e620285ffaaaccf3250b53220fc33714681de69c))
* support dag of sliced tasks in debug mode ([bddfbac](https://github.com/deepmodeling/dflow/commit/bddfbaca77b4dd4cc664726d50feb314c3531e96))
* support debug mode for executor ([2cfc37b](https://github.com/deepmodeling/dflow/commit/2cfc37bd48d3649da8e7f336d4e3a53e6c7223b7))
* use process instead of thread ([2612f6e](https://github.com/deepmodeling/dflow/commit/2612f6eeadb73e3488d82342bd3311d46e6a8c3f))
* wait process by queue.get() ([ff27daf](https://github.com/deepmodeling/dflow/commit/ff27daff693d9643f4eb9402f660c0262a798d82))

## [1.5.1](https://github.com/deepmodeling/dflow/compare/v1.5.0...v1.5.1) (2022-09-16)


### Bug Fixes

* mixed mode for lebesgue executor ([9fe9490](https://github.com/deepmodeling/dflow/commit/9fe9490e7e52bddb361a78aea79c5875ecab8f75))

## [1.5.0](https://github.com/deepmodeling/dflow/compare/v1.4.0...v1.5.0) (2022-09-07)


### Features

* **syntax-sugar:** Offer `with` context syntax for workflow, dag. ([898d4e3](https://github.com/deepmodeling/dflow/commit/898d4e3e509acba3c1dfaa0ce376e54a7a3ab64f))


### Bug Fixes

* can set slices for PythonOPTemplate after initialization ([81a1f43](https://github.com/deepmodeling/dflow/commit/81a1f436c3ff0a394e0bcfdeadbe179185c8ec95))
* pass save_as_aritfact for value_from_expression ([26cb790](https://github.com/deepmodeling/dflow/commit/26cb790bdd0faabf9825427dfa89fa2bcc2023cc))
* remove duplicate export class: S3Artifact ([5f4198d](https://github.com/deepmodeling/dflow/commit/5f4198d18a720dd03f6f1b5685e3da170ee6facb))
* restore availability of download_sliced_output_artifact ([9441971](https://github.com/deepmodeling/dflow/commit/944197122b4aa2de1051442b5a018f3616fce0b5))

## [1.4.0](https://github.com/deepmodeling/dflow/compare/v1.3.1...v1.4.0) (2022-09-04)


### Features

* expose plugin dependencies class and utils for better extensibility ([5a0c349](https://github.com/deepmodeling/dflow/commit/5a0c3498d473a2ec734136bdcab1839e9845a5e1))


### Bug Fixes

* a bug of handling multiple big parameters ([e8c35c1](https://github.com/deepmodeling/dflow/commit/e8c35c134d6196f7db39ff7c45ab7835c2d13628))
* add parallelism to steps and dag ([dc3ec98](https://github.com/deepmodeling/dflow/commit/dc3ec98c1424f52515a41da58a586ffa0555c424))
* avoid double rendering of output parameter which refers to the input parameter of the same template ([96a4e2e](https://github.com/deepmodeling/dflow/commit/96a4e2ef24acfab8977b846c8543f245eb678d94))
* decrease time cost of query keys ([d546ad5](https://github.com/deepmodeling/dflow/commit/d546ad535d3eda29e36494885f49a8ddf1bb6491))

## [1.3.1](https://github.com/deepmodeling/dflow/compare/v1.3.0...v1.3.1) (2022-09-01)


### Bug Fixes

* __getitem__ of ArgoVar handle non-string items ([08f4336](https://github.com/deepmodeling/dflow/commit/08f4336b70fb0ccb514898ef2e4950b4e7cc22bb))
* add method render_script to PythonOPTemplate ([8b2cb4d](https://github.com/deepmodeling/dflow/commit/8b2cb4def78f6ca4d4872c733239a7e7675bb4b5))
* add pod GC strategy ([4b85f90](https://github.com/deepmodeling/dflow/commit/4b85f904fffaee1e226238945320ae84810fc516))
* skip uploading non-existing files in dispatcher executor ([65b7730](https://github.com/deepmodeling/dflow/commit/65b773005b2cc7c569f3cd9b62685fc5dca097ad))

## [1.3.0](https://github.com/deepmodeling/dflow/compare/v1.2.7...v1.3.0) (2022-08-10)


### Features

* **Init Container:** Support init_container in op_template. ([77264b2](https://github.com/deepmodeling/dflow/commit/77264b259c383bcb90ed16e6b425efb649a482dc))
* **Ray Executor:** Ray executor with dependencies settings. ([7695ad8](https://github.com/deepmodeling/dflow/commit/7695ad8e66048769071aab77791b78105d58674d))


### Bug Fixes

* mkdir return 0 and upload failed ([71bd31b](https://github.com/deepmodeling/dflow/commit/71bd31bc19d8aa7ff6e99d8100e96253d2c28676))
* update latest yaml ([be88142](https://github.com/deepmodeling/dflow/commit/be881424320f65c0a8b25ffeb458b5fc3d58ebf2))

## [1.2.7](https://github.com/deepmodeling/dflow/compare/v1.2.6...v1.2.7) (2022-08-07)


### Bug Fixes

* add image_pull_policy for util images ([d08c0ac](https://github.com/deepmodeling/dflow/commit/d08c0ac38ffef7216bbbc7de1c2e07f219fda5bf))

## [1.2.6](https://github.com/deepmodeling/dflow/compare/v1.2.5...v1.2.6) (2022-08-05)


### Bug Fixes

* add sub_path and slice to download_artifact ([de9148b](https://github.com/deepmodeling/dflow/commit/de9148b3c6e9e432c1dfbca7a17b4e3ddd39d5bb))
* data persistent for minio ([de9148b](https://github.com/deepmodeling/dflow/commit/de9148b3c6e9e432c1dfbca7a17b4e3ddd39d5bb))
* handle big parameters of failed step ([de9148b](https://github.com/deepmodeling/dflow/commit/de9148b3c6e9e432c1dfbca7a17b4e3ddd39d5bb))

## [1.2.5](https://github.com/deepmodeling/dflow/compare/v1.2.4...v1.2.5) (2022-08-03)


### Bug Fixes

* add envs to script op template ([8ef468a](https://github.com/deepmodeling/dflow/commit/8ef468ad05fb52889f61e581b523f853191db411))
* add workflow actions: terminate, delete, resubmit, resume, retry, stop, suspend ([8ef468a](https://github.com/deepmodeling/dflow/commit/8ef468ad05fb52889f61e581b523f853191db411))

## [1.2.4](https://github.com/deepmodeling/dflow/compare/v1.2.3...v1.2.4) (2022-08-01)


### Bug Fixes

* add type to query conditions of workflow steps ([7528151](https://github.com/deepmodeling/dflow/commit/7528151db4d22cb0f4d96b0cf97668e42d2fe21d))
* avoid variable expansions in cat EOF ([7528151](https://github.com/deepmodeling/dflow/commit/7528151db4d22cb0f4d96b0cf97668e42d2fe21d))
* decoding error when read source file of OP ([7528151](https://github.com/deepmodeling/dflow/commit/7528151db4d22cb0f4d96b0cf97668e42d2fe21d))
* merge envs in dispatcher executor ([7528151](https://github.com/deepmodeling/dflow/commit/7528151db4d22cb0f4d96b0cf97668e42d2fe21d))

## [1.2.3](https://github.com/deepmodeling/dflow/compare/v1.2.2...v1.2.3) (2022-07-27)


### Bug Fixes

* bugs of reuse ([5290065](https://github.com/deepmodeling/dflow/commit/529006511913558b98fe717c2d5f3a13865a6d3a))
* github action of building docker image fails ([e40ac52](https://github.com/deepmodeling/dflow/commit/e40ac521f5a1c18f47f5370d4977d32d9459d20f))

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
