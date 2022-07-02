import json
import requests
from copy import deepcopy
from ..workflow import Workflow
from ..op_template import ShellOPTemplate, PythonScriptOPTemplate
from ..executor import Executor
from ..context import Context
from ..utils import randstr

class LebesgueExecutor(Executor):
    """
    Lebesgue executor

    Args:
        extra: extra arguments, will override extra defined in global context
    """
    def __init__(self, extra=None):
        self.extra = extra

    def render(self, template):
        assert "workflow.dp.tech/executor" in template.annotations, "lebesgue context not detected, lebesgue executor will not take effect"
        new_template = deepcopy(template)
        new_template.name += "-" + randstr()
        new_template.annotations["task.dp.tech/extra"] = json.dumps(self.extra) if isinstance(self.extra, dict) else self.extra
        return new_template

class LebesgueContext(Context):
    """
    Lebesgue context

    Args:
        username: user name for Lebesgue
        password: password for Lebesgue
        login_url: login url for Lebesgue
        app_name: application name
        org_id: organization ID
        user_id: user ID
        tag: tag
        executor: executor
        extra: extra arguments
        authorization: JWT token
    """
    def __init__(self, username=None, password=None, login_url="https://workflow.dp.tech/account_gw/login", app_name=None,
            org_id=None, user_id=None, tag=None, executor=None, extra=None, authorization=None):
        self.login_url = login_url
        self.username = username
        self.password = password
        self.app_name = app_name
        self.org_id = org_id
        self.user_id = user_id
        self.tag = tag
        self.executor = executor
        self.extra = extra
        if authorization is not None:
            self.authorization = authorization
        else:
            data = {
                "username": username,
                "password": password,
            }
            rsp = requests.post(login_url, headers={"Content-type": "application/json"}, json=data)
            res = json.loads(rsp.text)
            if res["code"] != 0:
                raise RuntimeError("Login failed: %s" % res["error"]["msg"])
            self.authorization = res["data"]["token"]

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
            new_template.annotations["workflow.dp.tech/executor"] = self.executor
            new_template.name += "-" + randstr()
            new_template.script = new_template.script.replace("/tmp", "$(pwd)/tmp")
            if isinstance(template, ShellOPTemplate):
                new_template.script = "mkdir -p tmp\n" + new_template.script
            if isinstance(template, PythonScriptOPTemplate):
                new_template.script = "import os\nos.makedirs('tmp', exist_ok=True)\n" + new_template.script
            return new_template

        return template
