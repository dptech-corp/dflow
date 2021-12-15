import os, shutil

def handle_output(output_parameter, output_artifact):
    os.makedirs('/tmp/outputs/parameters', exist_ok=True)
    os.makedirs('/tmp/outputs/artifacts', exist_ok=True)
    for name, value in output_parameter.items():
        open('/tmp/outputs/parameters/' + name, 'w').write(str(value))
    for name, value in output_artifact.items():
        os.makedirs('/tmp/outputs/artifacts/' + name, exist_ok=True)
        if isinstance(value, set):
            for path in value:
                target = "/tmp/outputs/artifacts/%s/%s" % (name, path) # --parents
                os.makedirs(os.path.dirname(target), exist_ok=True)
                create_hard_link(path, target)
        else:
            target = "/tmp/outputs/artifacts/%s/%s" % (name, value)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            create_hard_link(value, target)

def create_hard_link(src, dst):
    import os, shutil
    if os.path.isdir(src):
        shutil.copytree(src, dst, copy_function=os.link)
    elif os.path.isfile(src):
        os.link(src, dst)
    else:
        raise RuntimeError("File %s not found" % src)
