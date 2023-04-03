import sys
from pathlib import Path

from dflow.plugins.launching import OP_to_parser
from dflow.python import OP, OPIO, Artifact, OPIOSign, Parameter


"""@OP.function
def Duplicate(
    foo: Artifact(Path, description="input file"),
    num: Parameter(int, default=2, description="number"),
) -> {"bar": Artifact(Path)}:
    with open(foo, "r") as f:
        content = f.read()
    with open("bar.txt", "w") as f:
        f.write(content * num)
    return {"bar": Path("bar.txt")}"""


class Duplicate(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "foo": Artifact(Path, description="input file"),
            "num": Parameter(int, default=2, description="number"),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "bar": Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        with open(op_in["foo"], "r") as f:
            content = f.read()
        with open("bar.txt", "w") as f:
            f.write(content * op_in["num"])
        return OPIO({"bar": Path("bar.txt")})


to_parser = OP_to_parser(Duplicate)
if __name__ == '__main__':
    to_parser()(sys.argv[1:])
