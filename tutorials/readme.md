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
- WindowsOS: Coming Soon. [Submit your installation script here.](https://github.com/deepmodeling/dflow/issues/36)
- On Linux: Coming Soon. [Submit your installation script here.](https://github.com/deepmodeling/dflow/issues/37)

### Install Manually
#### Container engine
- Docker installation is very easy. Check out its official installation guide: [Get Docker](https://docs.docker.com/get-docker/)

#### Kubernetes
- If you are setting up Kubernetes on your own laptop, you can install minikube. Checkout its official installation guide: [minikube start](https://minikube.sigs.k8s.io/docs/start/)

#### Pydflow 
After you have installed the first two dependencies, you can instally dflow using pip. 
```bash
pip install pydflow
```

## Setup 
### Minikube
Dflow runs on kubernetes (k8s), so we need to start minikube
```bash
minikube start
```
If you have trouble starting minikube, checkout FAQ section.

### Argo
Dflow is built on [argo-workflow](https://github.com/argoproj/argo-workflows), so we need to setup argo engine in kubernetes or minikube:

1. To get started quickly, we can use the quick start manifest which will install Argo Workflows as well as some commonly used components:
```bash
kubectl create ns argo
kubectl apply -n argo -f https://raw.githubusercontent.com/deepmodeling/dflow/master/manifests/quick-start-postgres.yaml
```

2. To monitor the setup progress, we can checkout the pod status
```bash
kubectl get pod -n argo
```

**NOTE!!!!**: This process might take a while, depending on the internet speed. Wait and keep refreshing the above cell. Once the `STATUS` of all pods is `RUNNING`, you can proceed with the next step.

### Port-forward Argo UI and Minio API
1. Open a port-forward to access the Argo UI: 

**!!!!IMPORTANT!!!!** Since we need to keep this UI running, we have to keep this command running. 
    
```bash
kubectl -n argo port-forward deployment/argo-server 2746:2746 --address 0.0.0.0
```

2. Open a port-forward to access the minio API: 

**!!!!IMPORTANT!!!!** Open another terminal and run this, because you want to keep artifact respository running. Note that you don't need to ingress the artifact repository if you are not downloading or uploading artifact.

```bash
kubectl -n argo port-forward deployment/minio 9000:9000 --address 0.0.0.0
```

**BONUS** 3. Open a port-forward to access minio UI
```bash
kubectl -n argo port-forward deployment/minio 9001:9001 --address 0.0.0.0
```

<p align="center"> <strong> That's it! You've finished the installation and setup. </strong></p> 

# FAQ
**1. minikube start failure**
- Problem Description: After `minikube start`, you probably saw this message:
<p align="center">
<img src="./imgs/minikube_start_fail_bug.png" alt="minikube bug"/>
</p>

- Bug source: 

    - One common reason is that minikube starts with incorrect images. We can see the details of the log using the following command
    ```bash
    minikube ssh #enter minikube node 
    sudo journalctl -xeu kubelet
    ```
<p align="center">
<img src="./imgs/minikube_image_bug.png" alt="minikube bug"/>
</p>
NOTE: minikube needs `k8s.gcr.io/pause:3.6`, but it pulled `k8s.gcr.io/pause:3.7'. So it will not start.

- Solutions: 
    - (Recommended) Enter minikube environment and pull the image manually.
    ```bash 
    minikube ssh
    docker pull registry.aliyuncs.com/google_containers/pause:3.6
    docker tag registry.aliyuncs.com/google_containers/pause:3.6 k8s.gcr.io/pause:3.6
    exit
    ```
    - Downgrade minikube version: you can download older version in this page: [Releases · kubernetes/minikube](https://github.com/kubernetes/minikube/releases)