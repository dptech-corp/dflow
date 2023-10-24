from setuptools import setup

with open('VERSION', 'r') as f:
    VERSION = f.read()

with open('README.md', 'r') as f:
    LONG_DESCRIPTION = f.read()

setup(
    name='pydflow',
    version=VERSION,
    description='Dflow is a Python framework for constructing scientific '
    'computing workflows employing Argo Workflows as the workflow engine.',
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Xinzijian Liu",
    author_email="liuxzj@dp.tech",
    url="https://github.com/deepmodeling/dflow",
    license="LGPLv3",
    package_dir={'': 'src'},
    packages=[
        "dflow",
        "dflow/python",
        "dflow/python/vendor",
        "dflow/python/vendor/typeguard",
        "dflow/client",
        "dflow/plugins",
    ],
    include_package_data=True,
    python_requires='>=3.6',
    install_requires=[
        "six",
        "python-dateutil",
        "urllib3",
        "certifi",
        "argo-workflows==5.0.0",
        "jsonpickle",
        "minio",
        "kubernetes",
        "pyyaml",
        "cloudpickle==2.2.0",
        "requests",
        "tqdm",
        "psutil",
    ],
    entry_points={
        'console_scripts': ['dflow=dflow.main:main'],
    },
)
