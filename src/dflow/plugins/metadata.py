from typing import Dict, List, Union

from metadata import Dataset, MetadataContext
from metadata.entity.task import Task
from metadata.entity.workflow import WorkFlow

from .. import LineageClient


class MetadataClient(LineageClient):
    def __init__(self, gms_endpoint="https://datahub-gms.dp.tech", token=None):
        self.gms_endpoint = gms_endpoint
        self.token = token

    def register_workflow(
            self,
            workflow_name: str) -> str:
        with MetadataContext(endpoint=self.gms_endpoint, token=self.token) \
                as context:
            client = context.client
            workflow = WorkFlow(workflow_name)
            job = client.prepare_workflow(workflow)
            client.begin_workflow(job)
        return str(job.urn)

    def register_artifact(
            self,
            namespace: str,
            name: str,
            uri: str) -> str:
        with MetadataContext(endpoint=self.gms_endpoint, token=self.token) \
                as context:
            client = context.client
            urn = Dataset.gen_urn(context, namespace, name)
            ds = Dataset(urn=urn, display_name=name, uri=uri)
            client.update_dataset(ds)
        return urn

    def register_task(
            self,
            task_name: str,
            input_urns: Dict[str, Union[str, List[str]]],
            output_uris: Dict[str, str],
            workflow_urn: str) -> Dict[str, str]:
        with MetadataContext(endpoint=self.gms_endpoint, token=self.token) \
                as context:
            client = context.client
            task = Task(task_name, workflow_urn)
            job = client.prepare_job(task)
            inputs = []
            for urn in input_urns.values():
                if isinstance(urn, list):
                    inputs += urn
                elif isinstance(urn, str):
                    inputs.append(urn)
            run = client.begin_job(job, inputs=inputs)
            output_urns = {}
            for name, uri in output_uris.items():
                urn = Dataset.gen_urn(context, task_name, name)
                ds = Dataset(urn=urn, display_name=name, uri=uri)
                client.update_dataset(ds)
                output_urns[name] = urn
            client.end_job(run, outputs=list(output_urns.values()))
        return output_urns
