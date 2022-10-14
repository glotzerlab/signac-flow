# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os

from setuptools import find_packages, setup

requirements = [
    # The core package.
    "signac>=1.3.0",
    # For the templated generation of (submission) scripts.
    "jinja2>=3.0.0",
    # To enable the parallelized execution of operations across processes.
    "cloudpickle>=1.6.0",
    # Deprecation management
    "deprecation>=2.0.0",
    # Progress bars
    "tqdm>=4.60.0",
    # For schema validation
    "jsonschema>=3.0.0",
]

description = "Simple workflow management for signac projects."

try:
    this_path = os.path.dirname(os.path.abspath(__file__))
    fn_readme = os.path.join(this_path, "README.md")
    with open(fn_readme) as fh:
        long_description = fh.read()
except OSError:
    long_description = description

setup(
    name="signac-flow",
    version="0.22.0",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,
    maintainer="signac Developers",
    maintainer_email="signac-support@umich.edu",
    author="Carl Simon Adorf et al.",
    author_email="csadorf@umich.edu",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://signac.io",
    download_url="https://pypi.org/project/signac-flow/",
    keywords="workflow management signac framework database",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    entry_points={
        "console_scripts": [
            "flow = flow.__main__:main",
        ],
    },
    scripts=[
        "bin/simple-scheduler",
    ],
    install_requires=requirements,
    # Supported versions are determined according to NEP 29.
    # https://numpy.org/neps/nep-0029-deprecation_policy.html
    python_requires=">=3.8",
)
