import os
from typing import Dict, List, Optional, Union

from dp.metadata import Dataset, MetadataContext
from dp.metadata.entity.task import Task
from dp.metadata.entity.workflow import WorkFlow

from .. import LineageClient

config = {
    "gms_endpoint": os.environ.get("METADATA_GMS_ENDPOINT",
                                   "https://datahub-gms.dp.tech"),
    "project": os.environ.get("METADATA_PROJECT", None),
    "token": os.environ.get("METADATA_TOKEN", None),
}


class MetadataClient(LineageClient):
    def __init__(self, gms_endpoint=None, project=None, token=None):
        if gms_endpoint is None:
            gms_endpoint = config["gms_endpoint"]
        if project is None:
            project = config["project"]
        if token is None:
            token = config["token"]
        self.gms_endpoint = gms_endpoint
        self.project = project
        self.token = token

    def register_workflow(
            self,
            workflow_name: str) -> str:
        with MetadataContext(project=self.project, endpoint=self.gms_endpoint,
                             token=self.token) as context:
            client = context.client
            workflow = WorkFlow(workflow_name)
            job = client.prepare_workflow(workflow)
            client.begin_workflow(job)
        return str(job.urn)

    def register_artifact(
            self,
            namespace: str,
            name: str,
            uri: str,
            description: str = "",
            tags: Optional[List[str]] = None,
            properties: Optional[Dict[str, str]] = None,
            **kwargs) -> str:
        if tags is None:
            tags = []
        if properties is None:
            properties = {}
        with MetadataContext(project=self.project, endpoint=self.gms_endpoint,
                             token=self.token) as context:
            client = context.client
            urn = Dataset.gen_urn(context, namespace, name)
            ds = Dataset(urn=urn, display_name=name, uri=uri,
                         description=description, tags=tags,
                         properties=properties)
            client.update_dataset(ds)
        return urn

    def register_task(
            self,
            task_name: str,
            input_urns: Dict[str, Union[str, List[str]]],
            output_uris: Dict[str, str],
            workflow_urn: str) -> Dict[str, str]:
        with MetadataContext(project=self.project, endpoint=self.gms_endpoint,
                             token=self.token) as context:
            client = context.client
            task = Task(task_name, workflow_urn)
            job = client.prepare_job(task)
            inputs = []
            for urn in input_urns.values():
                if isinstance(urn, list):
                    inputs += urn
                elif isinstance(urn, str):
                    inputs.append(urn)
            inputs = list(filter(lambda x: x != "", inputs))
            run = client.begin_job(job, inputs=inputs)
            output_urns = {}
            for name, uri in output_uris.items():
                urn = Dataset.gen_urn(context, task_name, name)
                ds = Dataset(urn=urn, display_name=name, uri=uri)
                client.update_dataset(ds)
                output_urns[name] = urn
            client.end_job(run, outputs=list(output_urns.values()))
        return output_urns

    def get_artifact_metadata(self, urn: str) -> object:
        with MetadataContext(project=self.project, endpoint=self.gms_endpoint,
                             token=self.token) as context:
            client = context.client
            ds = client.get_dataset(urn)
        return ds
