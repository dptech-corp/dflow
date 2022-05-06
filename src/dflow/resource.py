from platform import node
import yaml

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

class SlurmJob(Resource):
    def __init__(self, header="", node_selector=None):
        self.header = header
        self.action = "create"
        self.success_condition = "status.status == Succeeded"
        self.failure_condition = "status.status == Failed"
        self.node_selector = node_selector

    def get_manifest(self, command, script):
        manifest = {
            "apiVersion": "wlm.sylabs.io/v1alpha1",
            "kind": "SlurmJob",
            "metadata": {
                "name": "{{pod.name}}"
            },
            "spec": {
                "batch": self.header + "\ncat <<EOF | " + " ".join(command) + "\n" + script + "\nEOF"
            }
        }
        if self.node_selector is not None:
            manifest["spec"]["nodeSelector"] = self.node_selector
        return yaml.dump(manifest)
