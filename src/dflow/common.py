from copy import deepcopy
try:
    from argo.workflows.client import V1alpha1S3Artifact
    from argo.workflows.client.configuration import Configuration
except:
    V1alpha1S3Artifact = object

class S3Artifact(V1alpha1S3Artifact):
    """
    S3 artifact

    Args:
        key: key of the s3 artifact
    """
    def __init__(self, path_list=None, *args, **kwargs):
        config = Configuration()
        config.client_side_validation = False
        super().__init__(local_vars_configuration=config, *args, **kwargs)
        self._sub_path = None
        self.path_list = path_list

    def sub_path(self, path):
        artifact = deepcopy(self)
        artifact._sub_path = path
        return artifact