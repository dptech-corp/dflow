# CHANGELOG

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
