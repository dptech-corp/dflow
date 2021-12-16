import os, shutil
import jsonpickle
from typing import Set
from .opio import ArtifactPath

def handle_output(output, sign):
    os.makedirs('/tmp/outputs/parameters', exist_ok=True)
    os.makedirs('/tmp/outputs/artifacts', exist_ok=True)
    for name, sign in sign.items():
        value = output[name]
        if sign == ArtifactPath:
            os.makedirs('/tmp/outputs/artifacts/' + name, exist_ok=True)
            target = "/tmp/outputs/artifacts/%s/%s" % (name, value)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            create_hard_link(value, target)
        elif sign == Set[ArtifactPath]:
            os.makedirs('/tmp/outputs/artifacts/' + name, exist_ok=True)
            for path in value:
                target = "/tmp/outputs/artifacts/%s/%s" % (name, path) # --parents
                os.makedirs(os.path.dirname(target), exist_ok=True)
                create_hard_link(path, target)
        elif sign == str:
            open('/tmp/outputs/parameters/' + name, 'w').write(value)
        else:
            open('/tmp/outputs/parameters/' + name, 'w').write(jsonpickle.dumps(value))

def create_hard_link(src, dst):
    import os, shutil
    if os.path.isdir(src):
        shutil.copytree(src, dst, copy_function=os.link)
    elif os.path.isfile(src):
        os.link(src, dst)
    else:
        raise RuntimeError("File %s not found" % src)
