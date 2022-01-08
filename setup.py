from setuptools import setup

setup(
    name='dflow',
    version='1.0.0',
    description='Dflow is a concurrent learning framework based on Argo Workflows.',
    author="Xinijian Liu",
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
        "argo-workflows",
        "jsonpickle",
        "minio",
    ]
)
