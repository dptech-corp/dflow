import re
from collections import UserDict, UserList

class ArgoObjectDict(UserDict):
    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError as e:
            raise AttributeError(e)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        if isinstance(value, dict):
            value = ArgoObjectDict(value)
        elif isinstance(value, list):
            value = ArgoObjectList(value)

        if key in ["parameters", "artifacts"]:
            value = {item.name: item for item in value}

        return value

class ArgoObjectList(UserList):
    def __getitem__(self, key):
        value = super().__getitem__(key)
        if isinstance(value, dict):
            value = ArgoObjectDict(value)
        elif isinstance(value, list):
            value = ArgoObjectList(value)
        return value

class ArgoWorkflow(ArgoObjectDict):
    def get_step(self, name=None, key=None):
        step_list = []
        if hasattr(self.status, "nodes"):
            for step in self.status.nodes.values():
                if name is not None and re.match(name, step["displayName"]) is None:
                    continue
                if key is not None:
                    if hasattr(step, "inputs") and hasattr(step.inputs, "parameters") \
                            and "dflow_key" in step.inputs.parameters and step.inputs.parameters["dflow_key"].value == str(key):
                        pass
                    else:
                        continue
                step_list.append(step)
        return step_list
