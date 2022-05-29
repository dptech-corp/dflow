class Resource(object):
    """
    Resource

    Args:
        action: action on the Kubernetes resource
        success_condition: expression representing success
        failure_condition: expression representing failure
    """
    action = None
    success_condition = None
    failure_condition = None
    def get_manifest(self, command, script):
        """
        The method to get the manifest (str)
        """
        raise NotImplementedError()
