.. _installation:

============
Installation
============

The recommended installation method for **signac-flow** is via conda_ or pip_.
The software is tested for Python versions 3.5+ and signac_ versions 1.0+.

.. _conda: https://conda.io/
.. _conda-forge: https://conda-forge.org/
.. _pip: https://pip.pypa.io/en/stable/
.. _signac: http://www.signac.io/

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

Alternatively you can clone the `git repository <https://github.com/glotzerlab/signac-flow>`_ and execute the ``setup.py`` script to install the package.

.. code:: bash

  git clone https://github.com/glotzerlab/signac-flow.git
  cd signac-flow
  python setup.py install --user
