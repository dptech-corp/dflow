import json
import os
from copy import deepcopy
from getpass import getpass
from typing import Optional

from ..config import boolize
from ..config import config as dflow_config
from ..config import s3_config
from ..context import Context
from ..executor import Executor, render_script_with_tmp_root
from ..op_template import PythonScriptOPTemplate, ShellOPTemplate
from ..utils import StorageClient, randstr
from ..workflow import Workflow
from .dispatcher import DispatcherArtifact

succ_code = [0, "0000"]
config = {
    "bohrium_url": os.environ.get("BOHRIUM_BOHRIUM_URL",
                                  "https://bohrium.dp.tech"),
    "username": os.environ.get("BOHRIUM_USERNAME", None),
    "phone": os.environ.get("BOHRIUM_PHONE", None),
    "password": os.environ.get("BOHRIUM_PASSWORD", None),
    "authorization": os.environ.get("BOHRIUM_AUTHORIZATION", None),
    "project_id": os.environ.get("BOHRIUM_PROJECT_ID", None),
    "tiefblue_url": os.environ.get("BOHRIUM_TIEFBLUE_URL",
                                   "https://tiefblue.dp.tech"),
    "ticket": os.environ.get("BOHRIUM_TICKET", None),
    "upload_progress": boolize(os.environ.get("BOHRIUM_UPLOAD_PROGRESS",
                                              False)),
}


def _raise_error(res, op):
    if res["code"] not in succ_code:
        if "error" in res:
            raise RuntimeError("%s failed: %s" % (op, res["error"]["msg"]))
        elif "message" in res:
            raise RuntimeError("%s failed: %s" % (op, res["message"]))
        else:
            raise RuntimeError("%s failed" % op)


def login(username=None, phone=None, password=None, bohrium_url=None):
    if username is None:
        username = config["username"]
    if phone is None:
        phone = config["phone"]
    if password is None:
        password = config["password"]
    if bohrium_url is None:
        bohrium_url = config["bohrium_url"]
    config["authorization"] = _login(
        bohrium_url + "/account/login", username, phone, password)
    update_headers()
    return config["authorization"]


def update_headers():
    headers = dflow_config["http_headers"]
    if config["ticket"] is not None:
        headers["Brm-Ticket"] = config["ticket"]
    elif config["authorization"] is not None:
        cookie = "brmToken=" + config["authorization"]
        if "Cookie" not in headers:
            headers["Cookie"] = cookie
        else:
            headers["Cookie"] += "; " + cookie
    if config["project_id"]:
        headers["Bohrium-Project-ID"] = config["project_id"]


def _login(login_url=None, username=None, phone=None, password=None):
    import requests
    if username is None and phone is None:
        username = input("Bohrium email: ")
        if not username:
            username = None
            phone = input("Bohrium phone: ")
    if password is None:
        password = getpass("Bohrium password: ")
    data = {
        "username": username,
        "phone": phone,
        "password": password,
    }
    rsp = requests.post(login_url, headers={
                        "Content-type": "application/json"}, json=data)
    res = json.loads(rsp.text)
    _raise_error(res, "login")
    return res["data"]["token"]


def create_job_group(job_group_name):
    import requests
    data = {
        "name": job_group_name,
    }
    if config["project_id"] is not None:
        data["projectId"] = int(config["project_id"])
    headers = {
        "Content-type": "application/json",
    }
    url = config["bohrium_url"] + "/brm/v1/job_group/add"
    if config["ticket"] is not None:
        headers["Brm-Ticket"] = config["ticket"]
        update_headers()
    else:
        authorization = login()
        headers["Authorization"] = "Bearer " + authorization
    rsp = requests.post(url, headers=headers, json=data)
    res = json.loads(rsp.text)
    _raise_error(res, "get job group id")
    return res["data"]["groupId"]


class BohriumExecutor(Executor):
    """
    Bohrium executor

    Args:
        executor: executor
        extra: extra arguments, will override extra defined in global context
    """

    def __init__(
            self,
            executor: Optional[str] = None,
            extra: Optional[dict] = None,
    ) -> None:
        self.executor = executor
        self.extra = extra

    def render(self, template):
        new_template = deepcopy(template)
        new_template.name += "-" + randstr()
        if self.executor is not None:
            new_template.annotations["workflow.dp.tech/executor"] = \
                self.executor
        if self.extra is not None:
            new_template.annotations["task.dp.tech/extra"] = json.dumps(
                self.extra) if isinstance(self.extra, dict) else self.extra

        new_template.script = render_script_with_tmp_root(template,
                                                          "$(pwd)/tmp")
        if isinstance(template, ShellOPTemplate):
            new_template.script = "mkdir -p tmp\n" + new_template.script
        if isinstance(template, PythonScriptOPTemplate):
            new_template.script = "import os\nos.makedirs('tmp', "\
                "exist_ok=True)\n" + new_template.script
        new_template.script_rendered = True
        return new_template


class BohriumContext(Context):
    """
    Bohrium context

    Args:
        username: email for Bohrium
        phone: phone number for Bohrium
        password: password for Bohrium
        bohrium_url: url for Bohrium
        executor: executor
        extra: extra arguments
        authorization: JWT token
    """

    def __init__(
            self,
            username: Optional[str] = None,
            phone: Optional[str] = None,
            password: Optional[str] = None,
            bohrium_url: Optional[str] = None,
            executor: Optional[str] = None,
            extra: Optional[dict] = None,
            authorization: Optional[str] = None,
    ) -> None:
        self.bohrium_url = bohrium_url if bohrium_url is not None else \
            config["bohrium_url"]
        self.login_url = self.bohrium_url + "/account/login"
        self.username = username if username is not None else \
            config["username"]
        self.phone = phone if phone is not None else \
            config["phone"]
        self.password = password if password is not None else \
            config["password"]
        self.authorization = authorization if authorization is not None else \
            config["authorization"]
        self.executor = executor
        self.extra = extra
        self.login()

    def login(self):
        if self.authorization is None:
            self.authorization = login(self.username, self.phone,
                                       self.password, self.bohrium_url)
            config["authorization"] = self.authorization

    def render(self, template):
        if isinstance(template, Workflow):
            template.annotations["workflow.dp.tech/executor"] = self.executor
            template.annotations["task.dp.tech/extra"] = json.dumps(
                self.extra) if isinstance(self.extra, dict) else self.extra
            template.annotations["workflow.dp.tech/authorization"] = \
                self.authorization
            template.annotations["workflow.dp.tech/executor_addr"] = \
                self.bohrium_url + "/"
            return template

        if isinstance(template, (ShellOPTemplate, PythonScriptOPTemplate)):
            new_template = deepcopy(template)
            new_template.name += "-" + randstr()
            new_template.annotations["workflow.dp.tech/executor"] = \
                self.executor
            if self.executor == "bohrium_v2":
                new_template.script = render_script_with_tmp_root(template,
                                                                  "$(pwd)/tmp")
                if isinstance(template, ShellOPTemplate):
                    new_template.script = "mkdir -p tmp\n" + \
                        new_template.script
                if isinstance(template, PythonScriptOPTemplate):
                    new_template.script = "import os\nos.makedirs('tmp', "\
                        "exist_ok=True)\n" + new_template.script
                new_template.script_rendered = True
            return new_template

        return template


class TiefblueClient(StorageClient):
    def __init__(
            self,
            bohrium_url: Optional[str] = None,
            username: Optional[str] = None,
            phone: Optional[str] = None,
            password: Optional[str] = None,
            authorization: Optional[str] = None,
            project_id: Optional[str] = None,
            token: Optional[str] = None,
            prefix: Optional[str] = None,
            sharePath: Optional[str] = None,
            userSharePath: Optional[str] = None,
            tiefblue_url: Optional[str] = None,
            ticket: Optional[str] = None,
    ) -> None:
        # only set s3_config["storage_client"] once
        if isinstance(s3_config["storage_client"], self.__class__):
            self.__dict__.update(s3_config["storage_client"].__dict__)
            return

        self.bohrium_url = bohrium_url if bohrium_url is not None else \
            config["bohrium_url"]
        self.username = username if username is not None else \
            config["username"]
        self.phone = phone if phone is not None else config["phone"]
        self.password = password if password is not None else \
            config["password"]
        self.authorization = authorization if authorization is not None else \
            config["authorization"]
        self.ticket = ticket if ticket is not None else config["ticket"]
        self.project_id = project_id if project_id is not None else \
            config["project_id"]
        self.tiefblue_url = tiefblue_url if tiefblue_url is not None else \
            config["tiefblue_url"]
        self.token = token
        self.prefix = prefix
        self.sharePath = sharePath
        self.userSharePath = userSharePath
        if self.token is None:
            self.get_token()
        s3_config["repo_type"] = "oss"
        s3_config["prefix"] = self.prefix
        if self.sharePath:
            s3_config["extra_prefixes"].append(self.sharePath)
        if self.userSharePath:
            s3_config["extra_prefixes"].append(self.userSharePath)

    def to_dict(self):
        retained_keys = ["bohrium_url",
                         "tiefblue_url", "username", "phone", "project_id"]
        return {k: self.__dict__[k] for k in retained_keys}

    def __getstate__(self):
        retained_keys = ["bohrium_url", "tiefblue_url", "project_id", "token",
                         "prefix", "sharePath", "userSharePath"]
        return {k: self.__dict__[k] for k in retained_keys}

    def __setstate__(self, d):
        self.__dict__.update(d)

    def get_token(self, retry=1):
        import requests
        url = self.bohrium_url + "/brm/v1/storage/token"
        headers = {
            "Content-type": "application/json",
        }
        params = {
            "projectId": self.project_id,
        }
        if self.ticket is not None:
            headers["Brm-Ticket"] = config["ticket"]
            update_headers()
        else:
            if self.authorization is None:
                self.authorization = login(
                    self.username, self.phone, self.password, self.bohrium_url)
            headers["Authorization"] = "Bearer " + self.authorization
        rsp = requests.get(url, headers=headers, params=params)
        if not rsp.text:
            if retry > 0:
                self.authorization = None
                self.get_token(retry=retry-1)
                return
            raise RuntimeError("Bohrium unauthorized")
        res = json.loads(rsp.text)
        _raise_error(res, "get storage token")
        self.token = res["data"]["token"]
        self.prefix = res["data"]["path"]
        self.sharePath = res["data"]["sharePath"]
        self.userSharePath = res["data"]["userSharePath"]

    def upload(self, key, path):
        try:
            import tiefblue
        except Exception:
            raise RuntimeError("Please install lbg utility by "
                               "`pip install -U lbg`")
        client = tiefblue.Client(base_url=self.tiefblue_url, token=self.token)
        try:
            client.upload_from_file(
                key, path, progress_bar=config["upload_progress"])
        except tiefblue.client.TiefblueException as e:
            if e.code == 190001:
                self.get_token()
                client = tiefblue.Client(base_url=self.tiefblue_url,
                                         token=self.token)
                client.upload_from_file(
                    key, path, progress_bar=config["upload_progress"])
            else:
                raise e

    def download(self, key, path):
        try:
            import tiefblue
        except Exception:
            raise RuntimeError("Please install lbg utility by "
                               "`pip install -U lbg`")
        client = tiefblue.Client(base_url=self.tiefblue_url, token=self.token)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            client.download_from_file(key, path)
        except tiefblue.client.TiefblueException as e:
            if e.code == 190001:
                self.get_token()
                client = tiefblue.Client(base_url=self.tiefblue_url,
                                         token=self.token)
                client.download_from_file(key, path)
            else:
                raise e

    def list(self, prefix, recursive=False):
        try:
            import tiefblue
        except Exception:
            raise RuntimeError("Please install lbg utility by "
                               "`pip install -U lbg`")
        client = tiefblue.Client(base_url=self.tiefblue_url, token=self.token)
        keys = []
        next_token = ""
        while True:
            try:
                res = client.list(prefix=prefix, recursive=recursive,
                                  next_token=next_token)
            except tiefblue.client.TiefblueException as e:
                if e.code == 190001:
                    self.get_token()
                    client = tiefblue.Client(base_url=self.tiefblue_url,
                                             token=self.token)
                    res = client.list(prefix=prefix, recursive=recursive,
                                      next_token=next_token)
                else:
                    raise e
            for obj in res["objects"]:
                if (recursive or obj["path"] == prefix) and \
                        obj["path"].endswith("/"):
                    continue
                keys.append(obj["path"])
            if not res["hasNext"]:
                break
            next_token = res["nextToken"]
        return keys

    def copy(self, src, dst):
        try:
            import tiefblue
        except Exception:
            raise RuntimeError("Please install lbg utility by "
                               "`pip install -U lbg`")
        client = tiefblue.Client(base_url=self.tiefblue_url, token=self.token)
        try:
            client.copy(src, dst)
        except tiefblue.client.TiefblueException as e:
            if e.code == 190001:
                self.get_token()
                client = tiefblue.Client(base_url=self.tiefblue_url,
                                         token=self.token)
                client.copy(src, dst)
            else:
                raise e

    def get_md5(self, key):
        try:
            import tiefblue
        except Exception:
            raise RuntimeError("Please install lbg utility by "
                               "`pip install -U lbg`")
        client = tiefblue.Client(base_url=self.tiefblue_url, token=self.token)
        try:
            meta = client.meta(key)
        except tiefblue.client.TiefblueException as e:
            if e.code == 190001:
                self.get_token()
                client = tiefblue.Client(base_url=self.tiefblue_url,
                                         token=self.token)
                meta = client.meta(key)
            else:
                raise e
        return meta["entityTag"] if "entityTag" in meta else ""


dflow_config["artifact_register"]["bohrium+datasets"] = \
    "dflow.plugins.bohrium.BohriumDatasetsArtifact"


class BohriumDatasetsArtifact(DispatcherArtifact):
    def __init__(self, path, sub_path=None):
        self.path = path
        self._sub_path = sub_path

    @classmethod
    def from_urn(cls, urn: str):
        path = urn
        if path.startswith("bohrium+datasets://"):
            path = path[19:]
        sub_path = None
        fields = path.split("/")
        if len(fields) > 4:
            path = "/".join(fields[:4])
            sub_path = "/".join(fields[4:])
        return cls(path=path, sub_path=sub_path)

    def get_urn(self) -> str:
        urn = "bohrium+datasets://%s" % self.path
        if self._sub_path is not None:
            urn += "/%s" % self._sub_path
        return urn

    def sub_path(self, path: str):
        artifact = deepcopy(self)
        if artifact._sub_path is None:
            artifact._sub_path = str(path)
        else:
            artifact._sub_path += "/%s" % path
        return artifact

    def modify_config(self, name: str, machine) -> str:
        if "dataset_path" not in machine.input_data:
            machine.input_data["dataset_path"] = []
        machine.input_data["dataset_path"].append(self.path)

    def bohrium_download(self, name: str, path: str):
        os.stat(self.path)
        if self._sub_path is not None:
            os.symlink("%s/%s" % (self.path, self._sub_path), path)
        else:
            os.symlink(self.path, path)

    def download(self, name: str, path: str):
        raise NotImplementedError()

    def remote_download(self, name: str, path: str):
        return NotImplementedError()
