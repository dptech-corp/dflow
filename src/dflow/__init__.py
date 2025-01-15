import logging
import os

from .argo_objects import ArgoStep, ArgoWorkflow
from .code_gen import gen_code
from .common import (CustomArtifact, HTTPArtifact, LineageClient,
                     LocalArtifact, S3Artifact, import_func, jsonpickle)
from .config import config, s3_config, set_config, set_s3_config
from .context import Context
from .dag import DAG
from .executor import ContainerExecutor, Executor, RemoteExecutor
from .io import (AutonamedDict, IfExpression, InputArtifact, InputParameter,
                 Inputs, OutputArtifact, OutputParameter, Outputs,
                 if_expression)
from .op_template import (HTTPOPTemplate, OPTemplate, PythonScriptOPTemplate,
                          Secret, ShellOPTemplate)
from .resource import Resource
from .slurm import SlurmJob, SlurmJobTemplate, SlurmRemoteExecutor
from .step import (HookStep, Step, argo_concat, argo_enumerate, argo_len,
                   argo_range, argo_sequence, argo_sum)
from .steps import Steps
from .task import Task
from .utils import (copy_artifact, copy_s3, download_artifact, download_s3,
                    path_list_of_artifact, path_object_of_artifact, randstr,
                    upload_artifact, upload_s3)
from .workflow import (DockerSecret, Workflow, parse_repo,
                       query_archived_workflows, query_workflows)

log_level = os.environ.get('LOG_LEVEL')
if log_level:
    logging.basicConfig(level=getattr(logging, log_level.upper(), None))

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
           "ArgoWorkflow", "argo_enumerate", "path_object_of_artifact",
           "CustomArtifact", "gen_code", "jsonpickle", "HTTPArtifact",
           "HookStep", "HTTPOPTemplate"]


if os.environ.get("DFLOW_LINEAGE"):
    config["lineage"] = import_func(os.environ.get("DFLOW_LINEAGE"))()
if os.environ.get("DFLOW_S3_STORAGE_CLIENT"):
    s3_config["storage_client"] = import_func(os.environ.get(
        "DFLOW_S3_STORAGE_CLIENT"))()
parse_repo()
