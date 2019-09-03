# <img src="https://raw.githubusercontent.com/glotzerlab/signac-flow/master/doc/images/logo.png" width="75" height="75"> signac-flow - manage workflows with signac

[![Affiliated with NumFOCUS](https://img.shields.io/badge/NumFOCUS-affiliated%20project-orange.svg?style=flat&colorA=E1523D&colorB=007D8A)](https://numfocus.org/sponsored-projects/affiliated-projects)
[![PyPI](https://img.shields.io/pypi/v/signac-flow.svg)](https://pypi.org/project/signac-flow/)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/signac-flow.svg?style=flat)](https://anaconda.org/conda-forge/signac-flow)
![CircleCI](https://img.shields.io/circleci/project/github/glotzerlab/signac-flow/master.svg)
[![RTD](https://img.shields.io/readthedocs/signac.svg?style=flat)](https://docs.signac.io)
[![License](https://img.shields.io/github/license/glotzerlab/signac-flow.svg)](https://github.com/glotzerlab/signac-flow/blob/master/LICENSE.txt)
[![conda-forge-downloads](https://img.shields.io/conda/dn/conda-forge/signac-flow.svg)](https://anaconda.org/conda-forge/signac-flow)
[![Gitter](https://img.shields.io/gitter/room/signac/Lobby.svg?style=flat)](https://gitter.im/signac/Lobby)

The [**signac** framework](https://signac.io) helps users manage and scale file-based workflows, facilitating data reuse, sharing, and reproducibility.

The **signac-flow** tool provides the basic components to set up simple to complex workflows for projects as part of the [signac framework](https://signac.io).
That includes the definition of data pipelines, execution of data space operations and the submission of operations to high-performance super computers.


## Resources

- [Framework documentation](https://docs.signac.io/):
  Examples, tutorials, topic guides, and package Python APIs.
- [Package documentation](https://docs.signac.io/projects/flow/):
  API reference for the signac-flow package.
- [Chat Support](https://gitter.im/signac/Lobby):
  Get help and ask questions on the **signac** gitter channel.
- [**signac** website](https://signac.io/):
  Framework overview and news.


## Installation

The recommended installation method for **signac-flow** is through **conda** or **pip**.
The software is tested for Python versions 3.5+ and is built for all major platforms.

To install **signac-flow** *via* the [conda-forge](https://conda-forge.github.io/) channel, execute:

```bash
conda install -c conda-forge signac-flow
```

To install **signac-flow** *via* **pip**, execute:

```bash
pip install signac-flow
```

**Detailed information about alternative installation methods can be found in the [documentation](https://docs.signac.io/en/latest/installation.html).**


## Testing

You can test this package by executing

    $ python -m unittest discover tests/

within the repository root directory.


## Acknowledgment

When using **signac** as part of your work towards a publication, we would really appreciate that you acknowledge **signac** appropriately.
We have prepared examples on how to do that [here](http://docs.signac.io/en/latest/acknowledge.html).
**Thank you very much!**

The signac framework is a [NumFOCUS Affiliated Project](https://numfocus.org/sponsored-projects/affiliated-projects).
