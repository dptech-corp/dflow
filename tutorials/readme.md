# Introduction 

There are several topics we intend to cover in this series:
- Installation and setup 
- Using dflow to write a simple workflow 
    - [dflow-helloworld.ipynb](https://github.com/deepmodeling/dflow/blob/master/tutorials/dflow-helloworld.ipynb)
    - [dflow-python.ipynb](https://github.com/deepmodeling/dflow/blob/master/tutorials/dflow-python.ipynb)
- Using dflow with to submit job on cluster
    - [dflow-slurm.ipynb](https://github.com/deepmodeling/dflow/blob/master/tutorials/dflow-slurm.ipynb)
- Advanced functionality of dflow
    - running jobs in parallel: [dflow-slices.ipynb](https://github.com/deepmodeling/dflow/blob/master/tutorials/dflow-slices.ipynb)
    - writing while loop in dflow: [dflow-recurse.ipynb](https://github.com/deepmodeling/dflow/blob/master/tutorials/dflow-recurse.ipynb)
    - reusing operated nodes: [dflow-reuse.ipynb](https://github.com/deepmodeling/dflow/blob/master/tutorials/dflow-reuse.ipynb)
    - complex condition: [dflow-conditional.ipynb](https://github.com/deepmodeling/dflow/blob/master/tutorials/dflow-conditional.ipynb)

# Installation and setup
## Installation
We need to install three dependencies to use dflow:
- Container engine: [Docker](https://www.docker.com/)([知乎介绍](https://zhuanlan.zhihu.com/p/23599229))
- Kubernetes: [minikube](https://minikube.sigs.k8s.io/docs/)([知乎介绍](https://zhuanlan.zhihu.com/p/112755080))
- dflow: [pydflow](https://pypi.org/project/pydflow/)

### Easy Install
You can use the installation script to install all dependencies in one step:
- MacOS: https://github.com/deepmodeling/dflow/blob/master/scripts/install-mac.sh
- WindowsOS: Coming Soon. Submit your installation script here.
- On Linux: Coming Soon. Submit your installation script here.

### Install Manually

## Setup 
installation and setup please checkout this page: [how to setup dflow on PC](https://zhuanlan.zhihu.com/p/528817338)
