config = {
    "host": "https://127.0.0.1:2746",
    "token": None,
    "k8s_config_file": None,
    "k8s_api_server": None,
    "private_key_host_path": "/home/docker/.ssh",
    "save_path_as_parameter": False,
    "catalog_dir_name": ".dflow",
    "archive_mode": "tar",
    "util_image": "python:3.8",
    "util_image_pull_policy": None,
    "extender_image": "dptechnology/dflow-extender",
    "extender_image_pull_policy": None,
    "dispatcher_image": "dptechnology/dpdispatcher",
    "dispatcher_image_pull_policy": None,
    "save_keys_in_global_outputs": True,
    "mode": "default",
}


def set_config(
    **kwargs,
) -> None:
    """
    Set global configurations

    Args:
        host: host of Argo server
        token: token for authentication, necessary for reused workflows
        k8s_config_file: location of kube config file if it is used for
        authentication
        k8s_api_server: address of Kubernetes API server, necessary for reused
        workflows
        private_key_host_path: path of private key on the Kubernetes nodes
        save_path_as_parameter: save catalog of artifacts as parameters
        catalog_dir_name: catalog directory name for artifacts
        archive_mode: "tar" for archiving with tar, None for no archive
        util_image: image for util step
        util_image_pull_policy: image pull policy for util step
        extender_image: image for dflow extender
        extender_image_pull_policy: image pull policy for dflow extender
        dispatcher_image: image for dpdispatcher
        dispatcher_image_pull_policy: image pull policy for dpdispatcher
        save_keys_in_global_outputs: save keys of steps in global outputs
        mode: "default" for normal, "debug" for debugging locally
    """
    config.update(kwargs)
