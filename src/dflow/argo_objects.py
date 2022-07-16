import os
import re
import tempfile
from collections import UserDict, UserList
from typing import Any, List, Union

import jsonpickle

from .io import S3Artifact
from .utils import download_artifact, download_s3, upload_artifact, upload_s3


class ArgoObjectDict(UserDict):
    """
    Generate ArgoObjectDict and ArgoObjectList on initialization rather than
    on __getattr__, otherwise modify a.b.c will not take effect
    """

    def __init__(self, d):
        super().__init__(d)
        for key, value in self.items():
            if isinstance(value, dict):
                self.data[key] = ArgoObjectDict(value)
            elif isinstance(value, list):
                self.data[key] = ArgoObjectList(value)

    def __getattr__(self, key):
        if key == "data":
            return super().__getattr__(key)

        if key in self.data:
            return self.data[key]
        else:
            raise AttributeError(
                "'ArgoObjectDict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        if key == "data":
            return super().__setattr__(key, value)

        self.data[key] = value

    def recover(self):
        return {key: value.recover() if isinstance(value, (ArgoObjectDict,
                                                           ArgoObjectList))
                else value for key, value in self.data.items()}


class ArgoObjectList(UserList):
    def __init__(self, li):
        super().__init__(li)
        for i, value in enumerate(self.data):
            if isinstance(value, dict):
                self.data[i] = ArgoObjectDict(value)
            elif isinstance(value, list):
                self.data[i] = ArgoObjectList(value)

    def recover(self):
        return [value.recover() if isinstance(value, (ArgoObjectDict,
                                                      ArgoObjectList))
                else value for value in self.data]


class ArgoStep(ArgoObjectDict):
    def __init__(self, step):
        super().__init__(step)
        self.key = None
        if hasattr(self, "inputs"):
            self.handle_io(self.inputs)
            if hasattr(self.inputs, "parameters") and "dflow_key" in \
                    self.inputs.parameters and self.inputs.parameters[
                        "dflow_key"].value != "":
                self.key = self.inputs.parameters["dflow_key"].value

        if hasattr(self, "outputs"):
            self.handle_io(self.outputs)

    def handle_io(self, io):
        if hasattr(io, "parameters"):
            parameters = {}
            for par in io.parameters:
                parameters[par.name] = par
                if hasattr(par, "value") and hasattr(par, "description") \
                        and par.description is not None:
                    desc = jsonpickle.loads(par.description)
                    if desc["type"] != str(str):
                        try:
                            parameters[par.name].value = jsonpickle.loads(
                                par.value)
                        except Exception:
                            pass
            io.parameters = parameters

        if hasattr(io, "artifacts"):
            io.artifacts = {art.name: art for art in io.artifacts}

        self.handle_big_parameters(io)

    def handle_big_parameters(self, io):
        if hasattr(io, "artifacts"):
            for name, art in io.artifacts.items():
                if name[:13] == "dflow_bigpar_":
                    if not hasattr(io, "parameters"):
                        io.parameters = {}
                    with tempfile.TemporaryDirectory() as tmpdir:
                        download_artifact(art, path=tmpdir)
                        fs = os.listdir(tmpdir)
                        assert len(fs) == 1
                        with open(os.path.join(tmpdir, fs[0]), "r") as f:
                            content = jsonpickle.loads(f.read())
                            param = {"name": name[13:],
                                     "save_as_artifact": True}
                            if "type" in content:
                                param["type"] = content["type"]
                            if "type" in content and content["type"] != \
                                    str(str):
                                param["value"] = jsonpickle.loads(
                                    content["value"])
                            else:
                                param["value"] = content["value"]
                            io.parameters[name[13:]] = ArgoObjectDict(param)

    def modify_output_parameter(
            self,
            name: str,
            value: Any,
    ) -> None:
        """
        Modify output parameter of an Argo step

        Args:
            name: parameter name
            value: new value
        """
        if isinstance(value, str):
            self.outputs.parameters[name].value = value
        else:
            self.outputs.parameters[name].value = jsonpickle.dumps(value)

        if hasattr(self.outputs.parameters[name], "save_as_artifact"):
            with tempfile.TemporaryDirectory() as tmpdir:
                path = tmpdir + "/" + name
                with open(path, "w") as f:
                    content = {"value": self.outputs.parameters[name].value}
                    if hasattr(self.outputs.parameters[name], "type"):
                        content["type"] = self.outputs.parameters[name].type
                    f.write(jsonpickle.dumps(content))
                key = upload_s3(path)
                s3 = S3Artifact(key=key)
                self.outputs.artifacts["dflow_bigpar_" + name].s3 = s3

    def modify_output_artifact(
            self,
            name: str,
            s3: S3Artifact,
    ) -> None:
        """
        Modify output artifact of an Argo step

        Args:
            name: artifact name
            s3: replace the artifact with a s3 object
        """
        assert isinstance(s3, S3Artifact), "must provide a S3Artifact object"
        self.outputs.artifacts[name].s3 = s3
        if s3.key[-4:] == ".tgz" and hasattr(self.outputs.artifacts[name],
                                             "archive"):
            del self.outputs.artifacts[name]["archive"]
        elif s3.key[-4:] != ".tgz" and not hasattr(self.outputs.artifacts[
                name], "archive"):
            self.outputs.artifacts[name]["archive"] = {"none": {}}

    def download_sliced_output_artifact(
            self,
            name: str,
            path: os.PathLike = ".",
    ) -> None:
        """
        Download output artifact of a sliced step

        Args:
            name: artifact name
            path: local path
        """
        assert (hasattr(self, "outputs") and
                hasattr(self.outputs, "parameters") and
                "dflow_%s_path_list" % name in self.outputs.parameters), \
            "%s is not sliced output artifact" % name
        path_list = jsonpickle.loads(
            self.outputs.parameters["dflow_%s_path_list" % name].value)
        for item in path_list:
            download_s3(self.outputs.artifacts[name].s3.key + "/" +
                        item["dflow_list_item"],
                        path=os.path.join(path, item["dflow_list_item"]))

    def upload_and_modify_sliced_output_artifact(
            self,
            name: str,
            path: Union[os.PathLike, List[os.PathLike]],
    ) -> None:
        """
        Upload and modify output artifact of a sliced step

        Args:
            name: artifact name
            path: local path to be uploaded
        """
        assert (hasattr(self, "outputs") and
                hasattr(self.outputs, "parameters") and
                "dflow_%s_path_list" % name in self.outputs.parameters), \
            "%s is not sliced output artifact" % name
        path_list = jsonpickle.loads(
            self.outputs.parameters["dflow_%s_path_list" % name].value)
        if not isinstance(path, list):
            path = [path]
        assert len(path_list) == len(path), "Require %s paths, %s paths"\
            " provided" % (len(path_list), len(path))
        path_list.sort(key=lambda x: x['order'])
        new_path = [None] * (path_list[-1]['order'] + 1)
        for local_path, item in zip(path, path_list):
            new_path[item["order"]] = local_path
        s3 = upload_artifact(new_path, archive=None)
        self.modify_output_artifact(name, s3)


class ArgoWorkflow(ArgoObjectDict):
    def get_step(
            self,
            name: str = None,
            key: str = None,
            phase: str = None,
            id: str = None,
    ) -> List[ArgoStep]:
        step_list = []
        if hasattr(self.status, "nodes"):
            for step in self.status.nodes.values():
                step = ArgoStep(step)
                if name is not None and re.match(name, step["displayName"])\
                        is None:
                    continue
                if key is not None and step.key != str(key):
                    continue
                if phase is not None and not (hasattr(step, "phase") and
                                              step.phase == phase):
                    continue
                if id is not None and step.id != id:
                    continue
                step_list.append(step)
        step_list.sort(key=lambda x: x["startedAt"])
        return step_list
