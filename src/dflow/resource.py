class Resource(object):
    """
    Resource
    :param action: action on the Kubernetes resource
    :param success_condition: expression representing success
    :param failure_condition: expression representing failure
    :method get_manifest: the method to get the manifest (str)
    """
    action = None
    success_condition = None
    failure_condition = None
    def get_manifest(self, command, script):
        raise NotImplementedError()
