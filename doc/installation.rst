.. _installation:

============
Installation
============

The recommended installation method for **signac-flow** is via conda_ or pip_.
The software is tested for Python versions 3.8+ and signac_ versions 1.3+.
The signac framework uses the `NEP 29 deprecation policy <https://numpy.org/neps/nep-0029-deprecation_policy.html>`__ as a guideline for when to drop support for Python and NumPy versions, and does not guarantee support beyond the versions recommended in that proposal.

.. _conda: https://conda.io/
.. _conda-forge: https://conda-forge.org/
.. _pip: https://pip.pypa.io/en/stable/
.. _signac: https://signac.readthedocs.io/

Install with conda
==================

You can install **signac-flow** via conda (available on the conda-forge_ channel), with:

.. code:: bash

    $ conda install -c conda-forge signac-flow

All additional dependencies will be installed automatically.
To upgrade the package, execute:

.. code:: bash

    $ conda update signac-flow


Install with pip
================

To install the package with the package manager pip_, execute

.. code:: bash

    $ pip install signac-flow --user

.. note::
    It is highly recommended to install the package into the user space and not as superuser!

To upgrade the package, simply execute the same command with the ``--upgrade`` option.

.. code:: bash

    $ pip install signac-flow --user --upgrade


Source Code Installation
========================

Alternatively you can clone the `git repository <https://github.com/glotzerlab/signac-flow>`_ and pip install it directly, or install directly from git.

.. code:: bash

  # Option 1
  git clone https://github.com/glotzerlab/signac-flow.git
  cd signac
  pip install .

  # Option 2
  pip install git+https://github.com/glotzerlab/signac-flow.git
