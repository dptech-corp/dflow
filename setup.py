from setuptools import setup

setup(
    name='clframe',
    version='1.0.0',
    description='Hera is a concurrent learning framework based on Argo Workflows.',
    author="Xinijian Liu",
    license="LGPLv3",
    package_dir={'': 'src'},
    packages=[
        "clframe",
        "clframe/python"
    ],
    python_requires='>=3.7',
    install_requires=[
        "six",
        "python-dateutil",
        "urllib3",
        "certifi",
        "typeguard",
        "argo-workflows"
    ]
)
