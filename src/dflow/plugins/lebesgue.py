from copy import deepcopy
from ..executor import Executor
from ..context import Context

class LebesgueExecutor(Executor):
    def __init__(self, authorization=None, image=None, command=None, log=None, inputs=None, outputs=None,
            resources=None, nodes=None, extra=None):
        self.authorization = authorization
        self.image = image
        self.command = command
        self.log = log
        self.inputs = inputs
        self.outputs = outputs
        self.resources = resources
        self.nodes = nodes
        self.extra = extra

    def render(self, template):
        new_template = deepcopy(template)
        new_template.name += "-lebesgue"

        if self.image is None:
            self.image = new_template.image
        if self.command is None:
            self.command = new_template.command
        self.script = new_template.script
        
        new_template.annotations["task.dp.tech/executor"] = "lebesgue"
        new_template.annotations["task.dp.tech/authorization"] = self.authorization
        new_template.annotations["task.dp.tech/image"] = self.image
        new_template.annotations["task.dp.tech/init"] = "cat <<EOF | %s\n%s\nEOF" % (" ".join(self.command), self.script)
        new_template.annotations["task.dp.tech/log"] = self.log
        new_template.annotations["task.dp.tech/inputs"] = self.inputs
        new_template.annotations["task.dp.tech/outputs"] = self.outputs
        new_template.annotations["task.dp.tech/resources"] = self.resources
        new_template.annotations["task.dp.tech/nodes"] = self.nodes
        new_template.annotations["task.dp.tech/extra"] = self.extra
        return new_template

class LebesgueContext(Context):
    def __init__(self, app_name=None, org_id=None, user_id=None, tag=None, executor=None, extra=None, authorization=None):
        self.app_name = app_name
        self.org_id = org_id
        self.user_id = user_id
        self.tag = tag
        self.executor = executor
        self.extra = extra
        self.authorization = authorization

    def get_annotations(self):
        return {
            "workflow.dp.tech/app_name": self.app_name,
            "workflow.dp.tech/org_id": self.org_id,
            "workflow.dp.tech/user_id": self.user_id,
            "workflow.dp.tech/tag": self.tag,
            "workflow.dp.tech/executor": self.executor,
            "task.dp.tech/extra": self.extra,
            "workflow.dp.tech/authorization": self.authorization
        }
