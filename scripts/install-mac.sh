#!/bin/bash

function INFO() {
    echo "[INFO] $@"
}

function WARNING() {
    echo >&2 "[WARNING] $@"
}

function ERROR() {
    echo >&2 "[ERROR] $@"
}

docker_path=$(which docker)
if [[ -n "$docker_path" ]]; then
    INFO "Found docker client at $docker_path"
else
    INFO "Docker client not found, installing docker client..."
    brew install docker
    if [[ $? != 0 ]]; then
        ERROR "Fail to install docker client"
        exit 1
    fi
fi

docker info 1>/dev/null 2>/dev/null
if [[ $? == 0 ]]; then
    INFO "Docker server has been running"
elif [[ $? == 1 ]]; then
    if [[ -d "/Applications/Docker.app" ]]; then
        INFO "Found docker app at /Applications/Docker.app"
    else
        INFO "Downloading docker app..."
        machine_type=$(uname -m)
        if [[ "$machine_type" == "amd64" || "$machine_type" == "x86_64" ]]; then
            curl -o Docker.dmg -L "https://desktop.docker.com/mac/main/amd64/Docker.dmg?utm_source=docker&utm_medium=webreferral&utm_campaign=docs-driven-download-mac-amd64"
        elif [[ "$machine_type" == "arm64" ]]; then
            curl -o Docker.dmg -L "https://desktop.docker.com/mac/main/arm64/Docker.dmg?utm_source=docker&utm_medium=webreferral&utm_campaign=docs-driven-download-mac-arm64"
        else
            ERROR "Unrecognized machine architecture"
        fi
        if [[ $? != 0 ]]; then
            ERROR "Fail to download docker"
            exit 1
        fi
        sudo hdiutil attach Docker.dmg
        sudo /Volumes/Docker/Docker.app/Contents/MacOS/install
        if [[ $? != 0 ]]; then
            ERROR "Fail to install docker"
            exit 1
        fi
        sudo hdiutil detach /Volumes/Docker
    fi

    INFO "Starting docker server"
    sudo open /Applications/Docker.app
    if [[ $? != 0 ]]; then
        ERROR "Fail to start docker app"
        exit 1
    fi
    while true; do
        docker info 1>/dev/null 2>/dev/null
        if [[ $? == 0 ]]; then
            INFO "Docker server has been running..."
            break
        fi
        INFO "Waiting docker server running..."
        sleep 3
    done
fi

minikube_path=$(which minikube)
if [[ -n "$minikube_path" ]]; then
    INFO "Found minikube binary at $minikube_path"
else
    INFO "Minikube not found, installing minikube..."
    machine_type=$(uname -m)
    if [[ "$machine_type" == "amd64" || "$machine_type" == "x86_64" ]]; then
        curl -o minikube -L "https://storage.googleapis.com/minikube/releases/latest/minikube-darwin-amd64"
    elif [[ "$machine_type" == "arm64" ]]; then
        curl -o minikube -L "https://storage.googleapis.com/minikube/releases/latest/minikube-darwin-arm64"
    else
        ERROR "Unrecognized machine architecture"
    fi
    if [[ $? != 0 ]]; then
        ERROR "Fail to download minikube"
        exit 1
    fi
    sudo install minikube /usr/local/bin/minikube
    if [[ $? != 0 ]]; then
        ERROR "Fail to install minikube"
        exit 1
    fi
fi

kubectl_path=$(which kubectl)
if [[ -z "$kubectl_path" ]]; then
    echo "alias kubectl=\"minikube kubectl --\"" >> ~/.bash_profile
    source ~/.bash_profile
fi

minikube status 1>/dev/null 2>/dev/null
if [[ $? < 2 ]]; then
    INFO "Minikube has been started"
else
    INFO "Starting minikube..."
    minikube start $@
    if [[ $? != 0 ]]; then
        ERROR "Fail to start minikube"
        exit 1
    fi
fi

kubectl create ns argo 1>/dev/null 2>/dev/null
kubectl apply -n argo -f https://raw.githubusercontent.com/deepmodeling/dflow/master/manifests/quick-start-postgres-3.4.1-deepmodeling.yaml 1>/dev/null
if [[ $? != 0 ]]; then
    ERROR "Fail to apply argo yaml"
    exit 1
fi

function waitForReady() {
    while true; do
        ready=$(kubectl get deployment $1 -n argo -o jsonpath='{.status.readyReplicas}')
        replicas=$(kubectl get deployment $1 -n argo -o jsonpath='{.status.replicas}')
        if [[ $? != 0 ]]; then
            ERROR "Fail to get status of $1"
            exit 1
        fi
        if [[ $replicas > 0 && $ready == $replicas ]]; then
            INFO "$1 has been ready..."
            break
        fi
        INFO "Waiting for $1 ready..."
        sleep 3
    done
}

waitForReady argo-server
waitForReady minio
waitForReady postgres
waitForReady workflow-controller

function forward() {
    pid=`ps -ef | grep port-forward | grep $1 | grep $2 | awk '{print $2}'`
    if [[ -n "$pid" ]]; then
        kill -9 $pid
    fi
    INFO "Forwarding $1:$2 to localhost:$2"
    nohup kubectl -n argo port-forward deployment/$1 $2:$2 --address 0.0.0.0 &
}
forward argo-server 2746
forward minio 9000
forward minio 9001

sleep 3
INFO "dflow server has been installed successfully!"
