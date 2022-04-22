from setuptools import setup

setup(
    name='pydflow',
    version='1.0.3',
    description='Dflow is a concurrent learning framework based on Argo Workflows.',
    author="Xinzijian Liu",
    license="LGPLv3",
    package_dir={'': 'src'},
    packages=[
        "dflow",
        "dflow/python",
        "dflow/client"
    ],
    python_requires='>=3.7',
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
    ]
)
