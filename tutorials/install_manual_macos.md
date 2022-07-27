# Install on MacOS
## Container Engine
There are different container engine. But we recommend to use Docker.
### Download 
Go to Docker official website to download: [Get Docker](https://docs.docker.com/desktop/install/mac-install/)
### Install
Find the `Docker.dmg` on the laptop. Install it as a common Mac software. 
### Verify the installation 
Run the `hello-world` official image to verify the installation.
```bash
docker run hello-world
```
This command downloads a test image and runs it in a container. When the
container runs, it prints a message and exits.

## Kubernetes
To use kubernetes on the laptop, we can install minikube, a local kubernetes.
### Download 

#### Up-to-date version
Go to minikube official website to download: [Minikube](https://minikube.sigs.k8s.io/docs/start/)
#### Older version
**NOTE: this is the recommended route for users in China**. The minikube image repository for the up-to-date version is not correct, which leads to minikube start failure. 
- Install older version from the official github repository: [Releases · kubernetes/minikube](https://github.com/kubernetes/minikube/releases)
- Install older version using [mirror in China](https://npmmirror.com/): [minikube](https://registry.npmmirror.com/binary.html?path=minikube/) (The latest version that works is 1.25.2)

Command to install minikube older version (1.25.2)
```bash
curl -o minikube -L https://registry.npmmirror.com/-/binary/minikube/v1.25.2/minikube-linux-amd64
```

#### Install 
Install you downloaded minikube to `/usr/local/bin`:
```bash
sudo install minikube /usr/local/bin/minikube
```

#### Verify the installation 
```bash
minikube version
```