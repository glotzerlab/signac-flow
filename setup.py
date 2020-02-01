# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
from setuptools import setup, find_packages


requirements = [
    # The core package.
    'signac>=1.0.0',
    # For the templated generation of (submission) scripts.
    'jinja2>=2.8',
    # To enable the parallelized execution of operations across processes.
    'cloudpickle',
    # Deprecation management
    'deprecation>=2',
    # For status view markdown rendering
    'mistune>=2.0',
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
    version='0.9.0',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,
    maintainer='signac Developers',
    maintainer_email='signac-support@umich.edu',
    author='Carl Simon Adorf et al.',
    author_email='csadorf@umich.edu',
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://signac.io",
    download_url="https://pypi.org/project/signac-flow/",
    keywords='workflow management signac framework database',

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Topic :: Scientific/Engineering :: Physics",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
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

    python_requires='>=3.5, <4',
)
