import os, shutil
import jsonpickle
from typing import Set, List
from pathlib import Path
from .opio import Artifact

def handle_input_artifacts(input, sign):
    for name, sign in sign.items():
        if isinstance(sign, Artifact):
            art_path = '/tmp/inputs/artifacts/%s' % name
            if sign.type == str:
                input[name] = art_path
            elif sign.type == Path:
                input[name] = Path(art_path)
            elif sign.type in [Set[str], Set[Path], List[str], List[Path]]:
                path_list = [art_path]
                if os.path.exists('%s/.dflow' % art_path):
                    with open('%s/.dflow' % art_path, 'r') as f:
                        path_list = jsonpickle.loads(f.read())['path_list']
                        path_list = list(map(lambda x: os.path.join(art_path, x), path_list))
                if sign.type == List[str]:
                    input[name] = path_list
                elif sign.type == Set[str]:
                    input[name] = set(path_list)
                elif sign.type == List[Path]:
                    input[name] = list(map(Path, path_list))
                elif sign.type == Set[Path]:
                    input[name] = set(map(Path, path_list))

def handle_output(output, sign):
    os.makedirs('/tmp/outputs/parameters', exist_ok=True)
    os.makedirs('/tmp/outputs/artifacts', exist_ok=True)
    for name, sign in sign.items():
        value = output[name]
        if isinstance(sign, Artifact):
            path_list = []
            if sign.type in [str, Path]:
                os.makedirs('/tmp/outputs/artifacts/' + name, exist_ok=True)
                target = "/tmp/outputs/artifacts/%s/%s" % (name, value)
                create_hard_link(value, target)
                path_list.append(str(value))
            elif sign.type in [List[str], List[Path], Set[str], Set[Path]]:
                os.makedirs('/tmp/outputs/artifacts/' + name, exist_ok=True)
                for path in value:
                    target = "/tmp/outputs/artifacts/%s/%s" % (name, path) # --parents
                    create_hard_link(path, target)
                    path_list.append(str(path))
            with open("/tmp/outputs/artifacts/%s/.dflow" % name, "w") as f:
                f.write(jsonpickle.dumps({"path_list": path_list}))
        elif sign == str:
            open('/tmp/outputs/parameters/' + name, 'w').write(value)
        else:
            open('/tmp/outputs/parameters/' + name, 'w').write(jsonpickle.dumps(value))

def create_hard_link(src, dst):
    import os, shutil
    os.makedirs(os.path.abspath(os.path.dirname(dst)), exist_ok=True)
    if os.path.isdir(src):
        shutil.copytree(src, dst, copy_function=os.link)
    elif os.path.isfile(src):
        os.link(src, dst)
    else:
        raise RuntimeError("File %s not found" % src)
