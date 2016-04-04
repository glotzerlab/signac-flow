import sys

if sys.version_info < (2, 7, 0):
    print("Error: signac requires python version >= 2.7.x.")
    sys.exit(1)

requirements = []
if sys.version_info < (3, 4, 0):
    requirements.append('enum34')

from setuptools import setup, find_packages

setup(
    name='signac-flow',
    version='0.1.8',
    packages=find_packages(),
    zip_safe=True,

    author='Carl Simon Adorf',
    author_email='csadorf@umich.edu',
    description="Workflow management based on signac.",
    keywords='workflow management singac framework database',

    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering :: Physics",
    ],

    install_requires=requirements,
)
