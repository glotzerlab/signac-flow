# README

[![Anaconda-Server Badge](https://anaconda.org/conda-forge/signac-flow/badges/version.svg)](https://anaconda.org/conda-forge/signac-flow)
[![PyPi](https://img.shields.io/pypi/v/signac-flow.svg)](https://img.shields.io/pypi/v/signac-flow.svg)

## About

Workflow management based on the signac framework.

The signac-flow tool provides the basic components to setup simple to complex workflows for [signac projects](https://glotzerlab.engin.umich.edu/signac).
That includes the definition of data pipelines, execution of data space operations and the submission of operations to high-performance super computers.

## Maintainers

  * Carl Simon Adorf (csadorf@umich.edu)
  * Paul Dodd (pdodd@umich.edu)
  * Vyas Ramasubramani (vramasub@umich.edu)

## Installation

The recommendend installation method for **signac-flow** is through **conda** or **pip**.
The software is tested for Python versions 2.7 and 3.5+ and is built for all major platforms.

This package is available via the [conda-forge](https://conda-forge.github.io/) conda channel:

`conda install -c conda-forge signac-flow`

or pip:

`pip install --user signac-flow`

## Documentation

The documentation for this package can be found online at [signac-flow.readthedocs.io](https://signac-flow.readthedocs.io/) or built manually using sphinx:
```
#!bash
cd doc
make html
```
