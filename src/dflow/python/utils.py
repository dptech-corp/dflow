import os
import shutil
import uuid
from pathlib import Path
from typing import List, Set

import jsonpickle

from ..config import config
from ..utils import (assemble_path_list, convert_dflow_list, copy_file,
                     remove_empty_dir_tag)
from .opio import BigParameter, Parameter


def handle_input_artifact(name, sign, slices=None, data_root="/tmp",
                          sub_path=None):
    if sub_path is None:
        art_path = '%s/inputs/artifacts/%s' % (data_root, name)
    else:
        art_path = '%s/inputs/artifacts/%s/%s' % (data_root, name, sub_path)
    if not os.path.exists(art_path):  # for optional artifact
        return None
    remove_empty_dir_tag(art_path)
    path_list = assemble_path_list(art_path)
    if slices is not None:
        slices = slices if isinstance(slices, list) else [slices]
        path_list = [path_list[i] for i in slices]
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


def handle_input_parameter(name, value, sign, slices=None, data_root="/tmp"):
    if "dflow_list_item" in value:
        dflow_list = []
        for item in jsonpickle.loads(value):
            dflow_list += jsonpickle.loads(item)
        obj = convert_dflow_list(dflow_list)
    elif isinstance(sign, BigParameter):
        with open(data_root + "/inputs/parameters/" + name, "r") as f:
            content = jsonpickle.loads(f.read())
            if sign.type == str:
                obj = content["value"]
            else:
                obj = jsonpickle.loads(content["value"])
    else:
        if isinstance(sign, Parameter):
            sign = sign.type
        if sign == str and slices is None:
            obj = value
        else:
            obj = jsonpickle.loads(value)

    if slices is not None:
        assert isinstance(
            obj, list), "Only parameters of type list can be sliced, while %s"\
            " is not list" % obj
        if isinstance(slices, list):
            obj = [obj[i] for i in slices]
        else:
            obj = obj[slices]

    return obj


def handle_output_artifact(name, value, sign, slices=None, data_root="/tmp"):
    path_list = []
    if sign.type in [str, Path]:
        os.makedirs(data_root + '/outputs/artifacts/' + name, exist_ok=True)
        if slices is not None:
            assert isinstance(slices, int)
        else:
            slices = 0
        if value and os.path.exists(str(value)):
            path_list.append({"dflow_list_item": copy_results(
                value, name, data_root), "order": slices})
        else:
            path_list.append({"dflow_list_item": None, "order": slices})
    elif sign.type in [List[str], List[Path], Set[str], Set[Path]]:
        os.makedirs(data_root + '/outputs/artifacts/' + name, exist_ok=True)
        if slices is not None:
            if isinstance(slices, int):
                for path in value:
                    path_list.append(copy_results_and_return_path_item(
                        path, name, slices, data_root))
            else:
                assert len(slices) == len(value)
                for path, s in zip(value, slices):
                    if isinstance(path, list):
                        for p in path:
                            path_list.append(
                                copy_results_and_return_path_item(p, name, s,
                                                                  data_root))
                    else:
                        path_list.append(copy_results_and_return_path_item(
                            path, name, s, data_root))
        else:
            for s, path in enumerate(value):
                path_list.append(copy_results_and_return_path_item(
                    path, name, s, data_root))

    os.makedirs(data_root + "/outputs/artifacts/%s/%s" % (name, config[
            "catalog_dir_name"]), exist_ok=True)
    with open(data_root + "/outputs/artifacts/%s/%s/%s" % (name, config[
            "catalog_dir_name"], uuid.uuid4()), "w") as f:
        f.write(jsonpickle.dumps({"path_list": path_list}))
    handle_empty_dir(data_root + "/outputs/artifacts/%s" % name)
    if config["save_path_as_parameter"]:
        with open(data_root + '/outputs/parameters/dflow_%s_path_list'
                  % name, 'w') as f:
            f.write(jsonpickle.dumps(path_list))


def handle_output_parameter(name, value, sign, slices=None, data_root="/tmp"):
    if slices is not None:
        if isinstance(slices, list):
            assert isinstance(value, list) and len(slices) == len(value)
            res = [{"dflow_list_item": v, "order": s}
                   for v, s in zip(value, slices)]
        else:
            res = [{"dflow_list_item": value, "order": slices}]
        with open(data_root + '/outputs/parameters/' + name, 'w') as f:
            f.write(jsonpickle.dumps(res))
    elif isinstance(sign, BigParameter):
        content = {"type": str(sign.type)}
        if sign.type == str:
            content["value"] = value
        else:
            content["value"] = jsonpickle.dumps(value)
        with open(data_root + "/outputs/parameters/" + name, "w") as f:
            f.write(jsonpickle.dumps(content))
    else:
        if isinstance(sign, Parameter):
            sign = sign.type
        if sign == str:
            with open(data_root + '/outputs/parameters/' + name, 'w') as f:
                f.write(value)
        else:
            with open(data_root + '/outputs/parameters/' + name, 'w') as f:
                f.write(jsonpickle.dumps(value))


def copy_results_and_return_path_item(path, name, order, data_root="/tmp"):
    if path and os.path.exists(str(path)):
        return {"dflow_list_item": copy_results(path, name, data_root),
                "order": order}
    else:
        return {"dflow_list_item": None, "order": order}


def copy_results(source, name, data_root="/tmp"):
    source = str(source)
    # if refer to input artifact
    if source.find(data_root + "/inputs/artifacts/") == 0:
        # retain original directory structure
        rel_path = source[source.find(
            "/", len(data_root + "/inputs/artifacts/"))+1:]
        target = data_root + "/outputs/artifacts/%s/%s" % (name, rel_path)
        copy_file(source, target, shutil.copy)
        if rel_path[:1] == "/":
            rel_path = rel_path[1:]
        return rel_path
    else:
        target = data_root + "/outputs/artifacts/%s/%s" % (name, source)
        try:
            copy_file(source, target, os.link)
        except Exception:
            copy_file(source, target, shutil.copy)
        if source[:1] == "/":
            source = source[1:]
        return source


def handle_empty_dir(path):
    # touch an empty file in each empty dir, as object storage will ignore
    # empty dirs
    for dn, ds, fs in os.walk(path, followlinks=True):
        if len(ds) == 0 and len(fs) == 0:
            with open(os.path.join(dn, ".empty_dir"), "w"):
                pass
