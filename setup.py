from setuptools import setup

setup(
    name='clframe',
    version='1.0.0',
    description='Hera is a concurrent learning framework based on Argo Workflows.',
    author="Xinijian Liu",
    license="LGPLv3",
    package_dir={'': 'src'},
    packages=[
        "clframe"
    ],
    python_requires='>=3.7',
    install_requires=[
        "argo-workflows"
    ]
)
