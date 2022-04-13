import os, re
import jsonpickle
from .io import S3Artifact
from .utils import download_s3, upload_artifact
from collections import UserDict, UserList

class ArgoObjectDict(UserDict):
    def __init__(self, d):
        super().__init__(d)
        for key, value in self.items():
            if isinstance(value, dict):
                self.data[key] = ArgoObjectDict(value)
            elif isinstance(value, list):
                self.data[key] = ArgoObjectList(value)

            if key in ["parameters", "artifacts"]:
                self.data[key] = {item.name: item for item in self.data[key]}

    def __getattr__(self, key):
        if key == "data":
            return super().__getattr__(key)

        if key in self.data:
            return self.data[key]
        else:
            raise AttributeError("'ArgoObjectDict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        if key == "data":
            return super().__setattr__(key, value)

        self.data[key] = value

class ArgoObjectList(UserList):
    def __init__(self, l):
        super().__init__(l)
        for i, value in enumerate(self.data):
            if isinstance(value, dict):
                self.data[i] = ArgoObjectDict(value)
            elif isinstance(value, list):
                self.data[i] = ArgoObjectList(value)

class ArgoStep(ArgoObjectDict):
    def __init__(self, step):
        super().__init__(step)
        if hasattr(self, "inputs") and hasattr(self.inputs, "parameters") \
                    and "dflow_key" in self.inputs.parameters and self.inputs.parameters["dflow_key"].value != "":
            self["key"] = self.inputs.parameters["dflow_key"].value
        else:
            self["key"] = None

    def modify_output_parameter(self, name, value):
        """
        Modify output parameter of an Argo step
        :param name: parameter name
        :param value: new value
        :return:
        """
        if isinstance(value, str):
            self.outputs.parameters[name].value = value
        else:
            self.outputs.parameters[name].value = jsonpickle.dumps(value)

    def modify_output_artifact(self, name, s3):
        """
        Modify output artifact of an Argo step
        :param name: artifact name
        :param s3: replace the artifact with a s3 object
        :return:
        """
        assert isinstance(s3, S3Artifact), "must provide a S3Artifact object"
        self.outputs.artifacts[name].s3 = s3
        if s3.key[-4:] == ".tgz" and hasattr(self.outputs.artifacts[name], "archive"):
            del self.outputs.artifacts[name]["archive"]
        elif s3.key[-4:] != ".tgz" and not hasattr(self.outputs.artifacts[name], "archive"):
            self.outputs.artifacts[name]["archive"] = {"none": {}}

    def download_sliced_output_artifact(self, name, path="."):
        """
        Download output artifact of a sliced step
        :param name: artifact name
        :param path: local path
        :return:
        """
        assert (hasattr(self, "outputs") and hasattr(self.outputs, "parameters") and "dflow_%s_path_list" % name in self.outputs.parameters), "%s is not sliced output artifact" % name
        path_list = jsonpickle.loads(self.outputs.parameters["dflow_%s_path_list" % name].value)
        for item in path_list:
            download_s3(self.outputs.artifacts[name].s3.key + "/" + item["dflow_list_item"], path=os.path.join(path, item["dflow_list_item"]))

    def upload_and_modify_sliced_output_artifact(self, name, path):
        """
        Upload and modify output artifact of a sliced step
        :param name: artifact name
        :param path: local path to be uploaded
        :return:
        """
        assert (hasattr(self, "outputs") and hasattr(self.outputs, "parameters") and "dflow_%s_path_list" % name in self.outputs.parameters), "%s is not sliced output artifact" % name
        path_list = jsonpickle.loads(self.outputs.parameters["dflow_%s_path_list" % name].value)
        if not isinstance(path, list):
            path = [path]
        assert len(path_list) == len(path), "Require %s paths, %s paths provided" % (len(path_list), len(path))
        path_list.sort(key=lambda x: x['order'])
        new_path = [None] * (path_list[-1]['order'] + 1)
        for local_path, item in zip(path, path_list):
            new_path[item["order"]] = local_path
        s3 = upload_artifact(new_path, archive=None)
        self.modify_output_artifact(name, s3)

class ArgoWorkflow(ArgoObjectDict):
    def get_step(self, name=None, key=None, phase=None, id=None):
        step_list = []
        if hasattr(self.status, "nodes"):
            for step in self.status.nodes.values():
                step = ArgoStep(step)
                if name is not None and re.match(name, step["displayName"]) is None:
                    continue
                if key is not None and step.key != str(key):
                    continue
                if phase is not None and not (hasattr(step, "phase") and step.phase == phase):
                    continue
                if id is not None and step.id != id:
                    continue
                step_list.append(step)
        step_list.sort(key=lambda x: x["startedAt"])
        return step_list
