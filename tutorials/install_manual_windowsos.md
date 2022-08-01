# Install and set up on WindowsOS
## Container engine: Docker
### Download
Go to the [Docker website](https://docs.docker.com/get-docker/) and download the **Docker Desktop for Windows**. Then you can follow the instructions on installer step by step.
### WSL Linux kernel
When you click and run docker desktop, you will see ***"Docker Desktop stopping"*** on the window. Wait a few seconds and then you will be informed that WSL 2 installation is incomplete with a [link](http://aka.ms/wsl2kernel) for the kernel update installation. After completing the instructions, you can restart Docker Desktop and see ***Docker Desktop running***

## Kubernetes
To use kubernetes on the laptop, we can install minikube, a local kubernetes.
### Download 
1. **Latest version**: Go to minikube official website to download: [Minikube](https://minikube.sigs.k8s.io/docs/start/)

2. **Older version**:
**this is the recommended version for users in China to avoid some conficts**. The minikube image repository for the up-to-date version is not correct, which leads to minikube start failure. 

You can find the releases from the official github repository: [Releases · kubernetes/minikube](https://github.com/kubernetes/minikube/releases). The latest version that works is [1.25.2](https://github.com/kubernetes/minikube/releases/tag/v1.25.2) and we can click it and download minikube-installer.exe of this version. 

### Verify the installation 
```bash
minikube version
```

