from typing import Optional

from .op_template import OPTemplate


class Resource(object):
    """
    Resource

    Args:
        action: action on the Kubernetes resource
        success_condition: expression representing success
        failure_condition: expression representing failure
    """
    action: Optional[str] = None
    success_condition: Optional[str] = None
    failure_condition: Optional[str] = None

    def get_manifest(
            self,
            template: OPTemplate,
    ) -> OPTemplate:
        """
        The method to get the manifest (str)
        """
        raise NotImplementedError()
