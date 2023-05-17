import os
from importlib import import_module

from .argo_objects import ArgoStep, ArgoWorkflow
from .common import LineageClient, LocalArtifact, S3Artifact
from .config import config, s3_config, set_config, set_s3_config
from .context import Context
from .dag import DAG
from .executor import ContainerExecutor, Executor, RemoteExecutor
from .io import (AutonamedDict, IfExpression, InputArtifact, InputParameter,
                 Inputs, OutputArtifact, OutputParameter, Outputs,
                 if_expression)
from .op_template import (OPTemplate, PythonScriptOPTemplate, Secret,
                          ShellOPTemplate)
from .resource import Resource
from .slurm import SlurmJob, SlurmJobTemplate, SlurmRemoteExecutor
from .step import (Step, argo_concat, argo_len, argo_range, argo_sequence,
                   argo_sum)
from .steps import Steps
from .task import Task
from .utils import (copy_artifact, copy_s3, download_artifact, download_s3,
                    path_list_of_artifact, randstr, upload_artifact, upload_s3)
from .workflow import (DockerSecret, Workflow, query_archived_workflows,
                       query_workflows)

__all__ = ["S3Artifact", "DAG", "Executor", "RemoteExecutor", "AutonamedDict",
           "IfExpression", "InputArtifact", "InputParameter", "Inputs",
           "OutputArtifact", "OutputParameter", "Outputs",
           "if_expression", "OPTemplate", "PythonScriptOPTemplate",
           "ShellOPTemplate", "Resource", "SlurmJob", "SlurmJobTemplate",
           "SlurmRemoteExecutor", "Step", "argo_len", "argo_range",
           "argo_sequence", "Steps", "Task", "copy_artifact", "copy_s3",
           "download_artifact", "download_s3", "path_list_of_artifact",
           "s3_config", "upload_artifact", "upload_s3", "Workflow", "config",
           "Context", "randstr", "LocalArtifact", "set_config",
           "set_s3_config", "DockerSecret", "argo_sum", "argo_concat",
           "LineageClient", "Secret", "query_workflows",
           "query_archived_workflows", "ContainerExecutor", "ArgoStep",
           "ArgoWorkflow"]


def import_func(s):
    fields = s.split(".")
    if fields[0] == __name__ or fields[0] == "":
        fields[0] = ""
        mod = import_module(".".join(fields[:-1]), package=__name__)
    else:
        mod = import_module(".".join(fields[:-1]))
    return getattr(mod, fields[-1])


if os.environ.get("DFLOW_LINEAGE"):
    config["lineage"] = import_func(os.environ.get("DFLOW_LINEAGE"))()
if os.environ.get("DFLOW_S3_STORAGE_CLIENT"):
    s3_config["storage_client"] = import_func(os.environ.get(
        "DFLOW_S3_STORAGE_CLIENT"))()
