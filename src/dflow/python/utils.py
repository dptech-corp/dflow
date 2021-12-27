import os, shutil
import uuid
import jsonpickle
from typing import Set, List
from pathlib import Path
from .opio import Artifact

def handle_input_artifact(name, sign, slices=None):
    art_path = '/tmp/inputs/artifacts/%s' % name
    if not os.path.exists(art_path):
        return None
    path_list = [art_path]
    catalog = list(filter(lambda x: x[:6] == ".dflow", os.listdir(art_path)))
    if len(catalog) == 1:
        with open('%s/%s' % (art_path, catalog[0]), 'r') as f:
            path_list = jsonpickle.loads(f.read())['path_list']
            path_list = list(map(lambda x: os.path.join(art_path, x), path_list))
    if slices is not None:
        if isinstance(slices, list):
            path_list = [path_list[i] for i in slices]
        else:
            path_list = [path_list[slices]]
    if sign.type == str:
        if len(path_list) == 1:
            return path_list[0]
        else:
            return art_path
    elif sign.type == Path:
        if len(path_list) == 1:
            return Path(path_list[0])
        else:
            return Path(art_path)
    elif sign.type == List[str]:
        return path_list
    elif sign.type == Set[str]:
        return set(path_list)
    elif sign.type == List[Path]:
        return list(map(Path, path_list))
    elif sign.type == Set[Path]:
        return set(map(Path, path_list))

def handle_output(output, sign):
    os.makedirs('/tmp/outputs/parameters', exist_ok=True)
    os.makedirs('/tmp/outputs/artifacts', exist_ok=True)
    for name, sign in sign.items():
        value = output[name]
        if isinstance(sign, Artifact):
            path_list = []
            if sign.type in [str, Path]:
                os.makedirs('/tmp/outputs/artifacts/' + name, exist_ok=True)
                path_list.append(copy_results(value, name))
            elif sign.type in [List[str], List[Path], Set[str], Set[Path]]:
                os.makedirs('/tmp/outputs/artifacts/' + name, exist_ok=True)
                for path in value:
                    path_list.append(copy_results(path, name))
            with open("/tmp/outputs/artifacts/%s/.dflow.%s" % (name, uuid.uuid4()), "w") as f:
                f.write(jsonpickle.dumps({"path_list": path_list}))
        elif sign == str:
            open('/tmp/outputs/parameters/' + name, 'w').write(value)
        else:
            open('/tmp/outputs/parameters/' + name, 'w').write(jsonpickle.dumps(value))

def copy_results(source, name):
    source = str(source)
    if source.find("/tmp/inputs/artifacts/") == 0: # if refer to input artifact
        rel_path = source[source.find("/", len("/tmp/inputs/artifacts/"))+1:] # retain original directory structure
        target = "/tmp/outputs/artifacts/%s/%s" % (name, rel_path)
        copy_file(source, target, shutil.copy)
        return rel_path
    else:
        target = "/tmp/outputs/artifacts/%s/%s" % (name, source)
        copy_file(source, target, os.link)
        return source

def copy_file(src, dst, func=os.link):
    os.makedirs(os.path.abspath(os.path.dirname(dst)), exist_ok=True)
    if os.path.isdir(src):
        shutil.copytree(src, dst, copy_function=func)
    elif os.path.isfile(src):
        func(src, dst)
    else:
        raise RuntimeError("File %s not found" % src)
