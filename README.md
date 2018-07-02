# README

[![Anaconda-Server Badge](https://anaconda.org/conda-forge/signac-flow/badges/version.svg)](https://anaconda.org/conda-forge/signac-flow)
[![PyPi](https://img.shields.io/pypi/v/signac-flow.svg)](https://img.shields.io/pypi/v/signac-flow.svg)
[![conda-forge-downloads](https://img.shields.io/conda/dn/conda-forge/signac-flow.svg)](https://anaconda.org/conda-forge/signac-flow)
[![RTD](https://readthedocs.org/projects/signac-flow/badge/?version=latest)](https://signac-flow.readthedocs.io)

## About

The **signac-flow** tool provides the basic components to setup simple to complex workflows for projects as part of the [signac framework](http://www.signac.io).
That includes the definition of data pipelines, execution of data space operations and the submission of operations to high-performance super computers.

## Maintainers

  * Carl Simon Adorf (csadorf@umich.edu)
  * Vyas Ramasubramani (vramasub@umich.edu)
  * Paul Dodd (pdodd@umich.edu)

## Installation

The recommendend installation method for **signac-flow** is through **conda** or **pip**.
The software is tested for Python versions 2.7 and 3.5+ and is built for all major platforms.

This package is available via the [conda-forge](https://conda-forge.github.io/) conda channel:

`conda install -c conda-forge signac-flow`

or pip:

`pip install --user signac-flow`

## Documentation

**If you are new to signac, check out the framework documentation at: [https://signac-docs.readthedocs.io](https://signac-docs.readthedocs.io)**

The documentation for this package can be found online at [signac-flow.readthedocs.io](https://signac-flow.readthedocs.io/) or built manually using sphinx:
```
#!bash
cd doc
make html
```

## Acknowledgment

When using **signac** as part of your work towards a publication, we would really appreciate that you acknowledge **signac** appropriately.
We have prepared examples on how to do that [here](http://signac-docs.readthedocs.io/en/latest/acknowledge.html).
**Thank you very much!**

## Testing

You can test this package by executing

    $ python -m unittest discover tests/

within the repository root directory.
