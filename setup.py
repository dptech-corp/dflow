from setuptools import setup

with open('VERSION', 'r') as f:
    VERSION = f.read()

setup(
    name='pydflow',
    version=VERSION,
    description='Dflow is a concurrent learning framework based on Argo Workflows.',
    author="Xinzijian Liu",
    license="LGPLv3",
    package_dir={'': 'src'},
    packages=[
        "dflow",
        "dflow/python",
        "dflow/client"
    ],
    python_requires='>=3.6',
    install_requires=[
        "six",
        "python-dateutil",
        "urllib3",
        "certifi",
        "typeguard",
        "argo-workflows==5.0.0",
        "jsonpickle",
        "minio",
        "kubernetes",
        "pyyaml"
    ]
)
