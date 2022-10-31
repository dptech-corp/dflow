import json
from copy import deepcopy
from getpass import getpass

import requests

from ..context import Context
from ..executor import Executor
from ..op_template import PythonScriptOPTemplate, ShellOPTemplate
from ..utils import randstr, s3_config
from ..workflow import Workflow

succ_code = [0, "0000"]
config = {
    "endpoint": "https://bohrium.dp.tech",
    "username": None,
    "password": None,
    "authorization": None,
    "project_id": None,
}


def _login(login_url=None, username=None, password=None):
    if username is None:
        username = input("Bohrium username: ")
    if password is None:
        password = getpass("Bohrium password: ")
    data = {
        "username": username,
        "password": password,
    }
    rsp = requests.post(login_url, headers={
                        "Content-type": "application/json"}, json=data)
    res = json.loads(rsp.text)
    if res["code"] not in succ_code:
        if "error" in res:
            raise RuntimeError("Login failed: %s" %
                               res["error"]["msg"])
        elif "message" in res:
            raise RuntimeError("Login failed: %s" % res["message"])
        else:
            raise RuntimeError("Login failed")
    return res["data"]["token"]


class BohriumExecutor(Executor):
    """
    Bohrium executor

    Args:
        executor: executor
        extra: extra arguments, will override extra defined in global context
    """

    def __init__(
            self,
            executor: str = None,
            extra: dict = None,
    ) -> None:
        self.executor = executor
        self.extra = extra

    def render(self, template):
        assert "workflow.dp.tech/executor" in template.annotations, \
            "bohrium context not detected, bohrium executor will "\
            "not take effect"
        new_template = deepcopy(template)
        new_template.name += "-" + randstr()
        if self.executor is not None:
            new_template.annotations["workflow.dp.tech/executor"] = \
                self.executor
        if self.extra is not None:
            new_template.annotations["task.dp.tech/extra"] = json.dumps(
                self.extra) if isinstance(self.extra, dict) else self.extra
        if self.executor == "bohrium_v2" and template.annotations[
                "workflow.dp.tech/executor"] != "bohrium_v2":
            new_template.script = new_template.script.replace(
                "/tmp", "$(pwd)/tmp")
            if isinstance(template, ShellOPTemplate):
                new_template.script = "mkdir -p tmp\n" + new_template.script
            if isinstance(template, PythonScriptOPTemplate):
                new_template.script = "import os\nos.makedirs('tmp', "\
                    "exist_ok=True)\n" + new_template.script
        return new_template


class BohriumContext(Context):
    """
    Bohrium context

    Args:
        username: user name for Bohrium
        password: password for Bohrium
        login_url: login url for Bohrium
        executor: executor
        extra: extra arguments
        authorization: JWT token
    """

    def __init__(
            self,
            username: str = None,
            password: str = None,
            login_url: str = None,
            executor: str = None,
            extra: dict = None,
            authorization: str = None,
    ) -> None:
        self.login_url = login_url if login_url is not None else \
            config["endpoint"] + "/account/login"
        self.username = username if username is not None else \
            config["username"]
        self.password = password if password is not None else \
            config["password"]
        self.authorization = authorization if authorization is not None else \
            config["authorization"]
        self.executor = executor
        self.extra = extra
        self.login()

    def login(self):
        if self.authorization is None:
            self.authorization = _login(self.login_url, self.username,
                                        self.password)
            config["authorization"] = self.authorization

    def render(self, template):
        if isinstance(template, Workflow):
            template.annotations["workflow.dp.tech/executor"] = self.executor
            template.annotations["task.dp.tech/extra"] = json.dumps(
                self.extra) if isinstance(self.extra, dict) else self.extra
            template.annotations["workflow.dp.tech/authorization"] = \
                self.authorization
            return template

        if isinstance(template, (ShellOPTemplate, PythonScriptOPTemplate)):
            new_template = deepcopy(template)
            new_template.name += "-" + randstr()
            new_template.annotations["workflow.dp.tech/executor"] = \
                self.executor
            if self.executor == "bohrium_v2":
                new_template.script = new_template.script.replace(
                    "/tmp", "$(pwd)/tmp")
                if isinstance(template, ShellOPTemplate):
                    new_template.script = "mkdir -p tmp\n" + \
                        new_template.script
                if isinstance(template, PythonScriptOPTemplate):
                    new_template.script = "import os\nos.makedirs('tmp', "\
                        "exist_ok=True)\n" + new_template.script
            return new_template

        return template


class TiefblueClient:
    def __init__(
            self,
            endpoint: str = None,
            username: str = None,
            password: str = None,
            authorization: str = None,
            project_id: str = None,
            token: str = None,
            prefix: str = None,
    ) -> None:
        self.endpoint = endpoint if endpoint is not None else \
            config["endpoint"]
        self.username = username if username is not None else \
            config["username"]
        self.password = password if password is not None else \
            config["password"]
        self.authorization = authorization if authorization is not None else \
            config["authorization"]
        self.project_id = project_id if project_id is not None else \
            config["project_id"]
        self.token = token
        self.prefix = prefix
        if self.token is None:
            self.get_token()
        s3_config["repo_type"] = "oss"
        s3_config["prefix"] = self.prefix

    def get_token(self):
        if self.authorization is None:
            self.authorization = _login(
                self.endpoint + "/account/login",
                self.username, self.password)
            config["authorization"] = self.authorization
        rsp = requests.get(
            self.endpoint + "/brm/v1/storage/token",
            headers={
                "Content-type": "application/json",
                "Authorization": "jwt " + self.authorization},
            params={"projectId": self.project_id})
        res = json.loads(rsp.text)
        self.token = res["data"]["token"]
        self.prefix = res["data"]["path"]

    def upload(self, key, path, **kwargs):
        import tiefblue
        client = tiefblue.Client(base_url="xxx", token=self.token)
        client.upload_from_file(key, path)

    def download(self, key, path, **kwargs):
        import tiefblue
        client = tiefblue.Client(base_url="xxx", token=self.token)
        client.download_from_file(key, path)

    def list(self, prefix, recursive=False):
        pass

    def copy(self, src, dst):
        pass

    def get_md5(self, key):
        pass
