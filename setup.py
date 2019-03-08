# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
from setuptools import setup, find_packages


requirements = [
    # The core package.
    'signac>=0.6',
    # For the templated generation of (submission) scripts.
    'jinja2',
    # To enable the parallelized execution of operations across processes.
    'cloudpickle',
    # To define IntEnum in flow/scheduling/base.py.
    'enum34;python_version<"3.4"',
    # Deprecation management
    'deprecation>=2',
]

description = "Simple workflow management for signac projects."

try:
    this_path = os.path.dirname(os.path.abspath(__file__))
    fn_readme = os.path.join(this_path, 'README.md')
    with open(fn_readme) as fh:
        long_description = fh.read()
except (IOError, OSError):
    long_description = description


with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name='signac-flow',
    version='0.6.4',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,

    author='Carl Simon Adorf',
    author_email='csadorf@umich.edu',
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://signac.io",
    keywords='workflow management signac framework database',

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    entry_points={
        'console_scripts': [
            'flow = flow.__main__:main',
        ],
    },

    scripts=[
        'bin/simple-scheduler',
    ],

    install_requires=requirements,

    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, <4',
)
