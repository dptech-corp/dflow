from .op_template import OPTemplate

class Steps(OPTemplate):
    def __init__(self, name, inputs=None, outputs=None, steps=None):
        super().__init__(name=name, inputs=inputs, outputs=outputs)
        if steps is not None:
            self.steps = steps
        else:
            self.steps = []

    def __iter__(self):
        return iter(self.steps)

    def add(self, step):
        self.steps.append(step)

