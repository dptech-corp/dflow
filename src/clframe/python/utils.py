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
                os.system("cp -lr --parents %s /tmp/outputs/artifacts/%s" % (path, name))
        else:
            os.system("cp -lr --parents %s /tmp/outputs/artifacts/%s" % (value, name))
