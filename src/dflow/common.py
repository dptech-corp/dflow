from copy import deepcopy
from typing import Any, Union

try:
    from argo.workflows.client import V1alpha1S3Artifact
    from argo.workflows.client.configuration import Configuration
except Exception:
    V1alpha1S3Artifact = object


class S3Artifact(V1alpha1S3Artifact):
    """
    S3 artifact

    Args:
        key: key of the s3 artifact
    """

    def __init__(
            self,
            path_list: Union[str, list] = None,
            *args,
            **kwargs,
    ) -> None:
        config = Configuration()
        config.client_side_validation = False
        super().__init__(local_vars_configuration=config, *args, **kwargs)
        if path_list is None:
            path_list = []
        self.path_list = path_list

    def sub_path(
            self,
            path: str,
    ) -> Any:
        artifact = deepcopy(self)
        if artifact.key[-1:] != "/":
            artifact.key += "/"
        artifact.key += path
        return artifact
