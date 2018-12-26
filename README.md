# README

[![Anaconda-Server Badge](https://anaconda.org/conda-forge/signac-flow/badges/version.svg)](https://anaconda.org/conda-forge/signac-flow)
[![PyPi](https://img.shields.io/pypi/v/signac-flow.svg)](https://img.shields.io/pypi/v/signac-flow.svg)
[![conda-forge-downloads](https://img.shields.io/conda/dn/conda-forge/signac-flow.svg)](https://anaconda.org/conda-forge/signac-flow)
[![RTD](https://readthedocs.org/projects/signac-flow/badge/?version=latest)](https://signac-flow.readthedocs.io)

## About

The **signac-flow** tool provides the basic components to set up simple to complex workflows for projects as part of the [signac framework](https://www.signac.io).
That includes the definition of data pipelines, execution of data space operations and the submission of operations to high-performance super computers.

## Maintainers

  * Carl Simon Adorf (csadorf@umich.edu)
  * Vyas Ramasubramani (vramasub@umich.edu)
  * Bradley D. Dice (bdice@umich.edu)

## Installation

The recommendend installation method for **signac-flow** is through **conda** or **pip**.
The software is tested for Python versions 2.7 and 3.4+ and is built for all major platforms.

This package is available via the [conda-forge](https://conda-forge.org/) conda channel:

`conda install -c conda-forge signac-flow`

or pip:

`pip install --user signac-flow`

## Documentation

The framework documentation is hosted at [https://docs.signac.io](https://docs.signac.io).

The API reference for this package is hosted at [https://docs.signac.io/projects/flow/](https://docs.signac.io/projects/flow/).

## Acknowledgment

When using **signac** as part of your work towards a publication, we would really appreciate that you acknowledge **signac** appropriately.
We have prepared examples on how to do that [here](https://docs.signac.io/en/latest/acknowledge.html).
**Thank you very much!**

## Testing

You can test this package by executing

    $ python -m unittest discover tests/

within the repository root directory.
