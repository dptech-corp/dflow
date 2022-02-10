import re
import jsonpickle
from .io import S3Artifact
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
                    and "dflow_key" in self.inputs.parameters:
            self["key"] = self.inputs.parameters["dflow_key"].value
        else:
            self["key"] = None

    def modify_output_parameter(self, name, value):
        if isinstance(value, str):
            self.outputs.parameters[name].value = value
        else:
            self.outputs.parameters[name].value = jsonpickle.dumps(value)

    def modify_output_artifact(self, name, s3):
        assert isinstance(s3, S3Artifact), "must provide a S3Artifact object"
        self.outputs.artifacts[name].s3 = s3

class ArgoWorkflow(ArgoObjectDict):
    def get_step(self, name=None, key=None):
        step_list = []
        if hasattr(self.status, "nodes"):
            for step in self.status.nodes.values():
                step = ArgoStep(step)
                if name is not None and re.match(name, step["displayName"]) is None:
                    continue
                if key is not None and step.key != str(key):
                    continue
                step_list.append(step)
        return step_list
