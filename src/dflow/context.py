import abc
from abc import ABC

from .op_template import OPTemplate


class Context(ABC):
    """
    Context
    """
    @abc.abstractmethod
    def render(
            self,
            template: OPTemplate,
    ) -> OPTemplate:
        """
        render original template and return a new template
        """
        return template
