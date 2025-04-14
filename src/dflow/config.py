import json
import os
import tempfile


def boolize(s):
    if isinstance(s, str):
        if s.lower() in ["", "false", "0"]:
            return False
        else:
            return True
    return s


def nullable(s):
    if isinstance(s, str) and s.lower() == "none":
        return None
    return s


def split_headers(s):
    if isinstance(s, str):
        headers = {}
        for h in s.split(";"):
            fields = h.split(":")
            if len(fields) == 2:
                headers[fields[0]] = fields[1]
        return headers
    return s


config = {
    "host": os.environ.get("DFLOW_HOST", "https://127.0.0.1:2746"),
    "namespace": os.environ.get("DFLOW_NAMESPACE", "argo"),
    "token": os.environ.get("DFLOW_TOKEN", None),
    "k8s_config_file": os.environ.get("DFLOW_K8S_CONFIG_FILE", None),
    "k8s_api_server": os.environ.get("DFLOW_K8S_API_SERVER", None),
    "private_key_host_path": os.environ.get("DFLOW_PRIVATE_KEY_HOST_PATH",
                                            None),
    "save_path_as_parameter": boolize(os.environ.get(
        "DFLOW_SAVE_PATH_AS_PARAMETER", False)),
    "catalog_dir_name": os.environ.get("DFLOW_CATALOG_DIR_NAME", ".dflow"),
    "archive_mode": nullable(os.environ.get("DFLOW_ARCHIVE_MODE", "tar")),
    "util_image": os.environ.get("DFLOW_UTIL_IMAGE", "python:3.8"),
    "util_image_pull_policy": os.environ.get("DFLOW_UTIL_IMAGE_PULL_POLICY",
                                             None),
    "extender_image": os.environ.get("DFLOW_EXTENDER_IMAGE",
                                     "dptechnology/dflow-extender"),
    "extender_image_pull_policy": os.environ.get(
        "DFLOW_EXTENDER_IMAGE_PULL_POLICY", None),
    "dispatcher_image": os.environ.get("DFLOW_DISPATCHER_IMAGE",
                                       "dptechnology/dpdispatcher"),
    "dispatcher_image_pull_policy": os.environ.get(
        "DFLOW_DISPATCHER_IMAGE_PULL_POLICY", None),
    "save_keys_in_global_outputs": boolize(os.environ.get(
        "DFLOW_SAVE_KEYS_IN_GLOBAL_OUTPUTS", False)),
    "mode": os.environ.get("DFLOW_MODE", "default"),
    "lineage": None,
    "register_tasks": boolize(os.environ.get("DFLOW_REGISTER_TASKS", False)),
    "http_headers": split_headers(os.environ.get("DFLOW_HTTP_HEADERS", {})),
    "workflow_annotations": json.loads(os.environ.get(
        "DFLOW_WORKFLOW_ANNOTATIONS", "{}")),
    "overwrite_reused_artifact": boolize(os.environ.get(
        "DFLOW_OVERWRITE_REUSED_ARTIFACT", True)),
    "detach": boolize(os.environ.get("DFLOW_DETACH", False)),
    "debug_copy_method": os.environ.get("DFLOW_DEBUG_COPY_METHOD", "symlink"),
    "debug_pool_workers": (lambda s: None if s is None else int(s))(
        os.environ.get("DFLOW_DEBUG_POOL_WORKERS", None)),
    "debug_batch_size": (lambda s: None if s is None else int(s))(
        os.environ.get("DFLOW_DEBUG_BATCH_SIZE", None)),
    "debug_batch_interval": int(os.environ.get("DFLOW_DEBUG_BATCH_INTERVAL",
                                               30)),
    "detect_empty_dir": boolize(os.environ.get("DFLOW_DETECT_EMPTY_DIR",
                                               True)),
    "artifact_register": {},
    "debug_s3": boolize(os.environ.get("DFLOW_DEBUG_S3", False)),
    "debug_workdir": os.environ.get("DFLOW_DEBUG_WORKDIR", "."),
    "debug_artifact_dir": os.environ.get("DFLOW_DEBUG_ARTIFACT_DIR", "."),
    "debug_failfast": boolize(os.environ.get("DFLOW_DEBUG_FAILFAST", False)),
    "debug_save_copy_method": os.environ.get("DFLOW_DEBUG_SAVE_COPY_METHOD",
                                             "symlink"),
    "raise_for_group": boolize(os.environ.get("DFLOW_RAISE_FOR_GROUP", False)),
    "dispatcher_debug": boolize(os.environ.get("DISPATCHER_DEBUG", False)),
    "dereference_symlink": boolize(os.environ.get("DFLOW_DEREFERENCE_SYMLINK",
                                                  False)),
}


def set_config(
    **kwargs,
) -> None:
    """
    Set global configurations

    Args:
        host: host of Argo server
        namespace: k8s namespace
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
        lineage: lineage client, None by default
        http_headers: HTTP headers for requesting Argo server
        workflow_annotations: default annotations for workflows
        overwrite_reused_artifact: overwrite reused artifact
    """
    config.update(kwargs)


s3_config = {
    "endpoint": os.environ.get("DFLOW_S3_ENDPOINT", "127.0.0.1:9000"),
    "console": os.environ.get("DFLOW_S3_CONSOLE", "http://127.0.0.1:9001"),
    "access_key": os.environ.get("DFLOW_S3_ACCESS_KEY", "admin"),
    "secret_key": os.environ.get("DFLOW_S3_SECRET_KEY", "password"),
    "secure": boolize(os.environ.get("DFLOW_S3_SECURE", False)),
    "bucket_name": os.environ.get("DFLOW_S3_BUCKET_NAME", "my-bucket"),
    "repo_key": os.environ.get("DFLOW_S3_REPO_KEY", None),
    "repo": json.loads(os.environ.get("DFLOW_S3_REPO", "null")),
    "repo_type": os.environ.get("DFLOW_S3_REPO_TYPE", "s3"),
    "repo_prefix": os.environ.get("DFLOW_S3_REPO_PREFIX", ""),
    "prefix": os.environ.get("DFLOW_S3_PREFIX", ""),
    "storage_client": None,
    "extra_prefixes": os.environ.get("DFLOW_S3_EXTRA_PREFIXES").split(";") if
    os.environ.get("DFLOW_S3_EXTRA_PREFIXES") else [],
}


def set_s3_config(
    **kwargs,
) -> None:
    """
    Set S3 configurations

    Args:
        endpoint: endpoint for S3 storage
        console: console address for S3 storage
        access_key: access key for S3 storage
        secret_key: secret key for S3 storage
        secure: secure or not
        bucket_name: name of S3 bucket
        repo_key: key of artifact repository
        repo: artifact repository, parsed from repo_key
        repo_type: s3 or oss, parsed from repo_key
        repo_prefix: prefix of artifact repository, parsed from repo_key
        prefix: prefix of storage key
        storage_client: client for plugin storage backend
        extra_prefixes: extra prefixes ignored by auto-prefixing
    """
    s3_config.update(kwargs)


if os.environ.get("DFLOW_TEMPDIR"):
    tempfile.tempdir = os.environ.get("DFLOW_TEMPDIR")
