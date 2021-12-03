from copy import deepcopy

class AutonamedDict(dict):
    def __init__(self, d=None):
        super().__init__()
        if isinstance(d, dict):
            for key, value in d.items():
                self.__setitem__(key, value)

    def __setitem__(self, key, value):
        value = deepcopy(value)
        value.name = key
        super().__setitem__(key, value)

class Inputs:
    def __init__(self, parameters=None, artifacts=None):
        if parameters is not None:
            self.parameters = parameters
        else:
            self.parameters = AutonamedDict()
        
        if artifacts is not None:
            self.artifacts = artifacts
        else:
            self.artifacts = AutonamedDict()
    
    def __setattr__(self, key, value):
        if key in ["parameters", "artifacts"] and isinstance(value, dict):
            super().__setattr__(key, AutonamedDict(value))
        else:
            super().__setattr__(key, value)
    
    def set_step_id(self, step_id):
        for par in self.parameters.values():
            par.step_id = step_id
        for art in self.artifacts.values():
            art.step_id = step_id

class Outputs:
    def __init__(self, parameters=None, artifacts=None):
        if parameters is not None:
            self.parameters = parameters
        else:
            self.parameters = AutonamedDict()
        
        if artifacts is not None:
            self.artifacts = artifacts
        else:
            self.artifacts = AutonamedDict()
    
    def __setattr__(self, key, value):
        if key in ["parameters", "artifacts"] and isinstance(value, dict):
            super().__setattr__(key, AutonamedDict(value))
        else:
            super().__setattr__(key, value)
    
    def set_step_id(self, step_id):
        for par in self.parameters.values():
            par.step_id = step_id
        for art in self.artifacts.values():
            art.step_id = step_id

class InputParameter:
    def __init__(self, name=None, step_id=None, type=None, value=None):
        self.name = name
        self.step_id = step_id
        self.type = type
        self.value = value
    
    def __repr__(self):
        if self.name is not None:
            if self.step_id is not None:
                return "%s.inputs.parameters.%s" % (self.step_id, self.name)
            return "inputs.parameters.%s" % self.name
        return ""

class InputArtifact:
    def __init__(self, path, name=None, step_id=None, type=None, source=None):
        self.path = path
        self.name = name
        self.step_id = step_id
        self.type = type
        self.source = source
    
    def __repr__(self):
        if self.name is not None:
            if self.step_id is not None:
                return "%s.inputs.artifacts.%s" % (self.step_id, self.name)
            return "inputs.artifacts.%s" % self.name
        return ""

class OutputParameter:
    def __init__(self, value_from_path, name=None, step_id=None, type=None):
        self.value_from_path = value_from_path
        self.name = name
        self.step_id = step_id
        self.type = type
    
    def __repr__(self):
        if self.name is not None:
            if self.step_id is not None:
                return "%s.outputs.parameters.%s" % (self.step_id, self.name)
            return "outputs.parameters.%s" % self.name
        return ""

class OutputArtifact:
    def __init__(self, path, name=None, step_id=None, type=None, save=None):
        self.path = path
        self.name = name
        self.step_id = step_id
        self.type = type
        self.save = save

    def __repr__(self):
        if self.name is not None:
            if self.step_id is not None:
                return "%s.outputs.artifacts.%s" % (self.step_id, self.name)
            return "outputs.artifacts.%s" % self.name
        return ""