import json
from copy import deepcopy
from ..workflow import Workflow
from ..op_template import ShellOPTemplate, PythonScriptOPTemplate
from ..executor import Executor
from ..context import Context

class LebesgueExecutor(Executor):
    def __init__(self, extra=None):
        self.extra = extra

    def render(self, template):
        new_template = deepcopy(template)
        new_template.annotations["task.dp.tech/extra"] = json.dumps(self.extra) if isinstance(self.extra, dict) else self.extra
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

    def render(self, template):
        if isinstance(template, Workflow):
            template.annotations["workflow.dp.tech/app_name"] = self.app_name
            template.annotations["workflow.dp.tech/org_id"] = self.org_id
            template.annotations["workflow.dp.tech/user_id"] = self.user_id
            template.annotations["workflow.dp.tech/tag"] = self.tag
            template.annotations["workflow.dp.tech/executor"] = self.executor
            template.annotations["task.dp.tech/extra"] = json.dumps(self.extra) if isinstance(self.extra, dict) else self.extra
            template.annotations["workflow.dp.tech/authorization"] = self.authorization
            return template

        if isinstance(template, (ShellOPTemplate, PythonScriptOPTemplate)):
            new_template = deepcopy(template)
            new_template.script = new_template.script.replace("/tmp", "tmp")
            if isinstance(template, ShellOPTemplate):
                new_template.script = "mkdir -p tmp\n" + new_template.script
            if isinstance(template, PythonScriptOPTemplate):
                new_template.script = "import os\nos.makedirs('tmp', exist_ok=True)\n" + new_template.script
            return new_template

        return template
