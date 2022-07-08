from .op_template import OPTemplate


class Context(object):
    """
    Context
    """

    def render(
            self,
            template: OPTemplate,
    ) -> OPTemplate:
        """
        render original template and return a new template
        """
        return template
