import os, shutil

def handle_output(output_parameter, output_artifact):
    os.makedirs('/tmp/outputs/parameters', exist_ok=True)
    os.makedirs('/tmp/outputs/artifacts', exist_ok=True)
    for name, value in output_parameter.items():
        open('/tmp/outputs/parameters/' + name, 'w').write(str(value))
    for name, value in output_artifact.items():
        if isinstance(value, set):
            os.makedirs('/tmp/outputs/artifacts/' + name, exist_ok=True)
            for i, path in enumerate(value):
                create_hard_link(path, '/tmp/outputs/artifacts/%s/%s' % (name, i))
        else:
            create_hard_link(value, '/tmp/outputs/artifacts/' + name)

def create_hard_link(src, dst):
    if os.path.isdir(src):
        shutil.copytree(src, dst, copy_function=os.link)
    elif os.path.isfile(src):
        os.link(src, dst)
    else:
        raise RuntimeError("File %s not found" % src)