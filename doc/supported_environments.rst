.. _supported-environments:

======================
Supported Environments
======================

The **signac-flow** package streamlines the submission of job-operations to a supercomputer scheduling system.
We use a different template for the different schedulers and systems, but all templates are extending the :ref:`base-script`.

.. _base-script: supported_environments/base

.. toctree::
    :hidden:

    supported_environments/base

The package currently ships with drivers for:

.. toctree::

   supported_environments/slurm
   supported_environments/torque
   supported_environments/lsf

Any supercomputing system utilizing these schedulers is supported out of the box.
In addition, the package provides specialized submission profiles for the following environments:

.. toctree::
   supported_environments/summit
   supported_environments/titan
   supported_environments/eos
   supported_environments/stampede2
   supported_environments/comet
   supported_environments/bridges
   supported_environments/umich

To use these environment profiles, make sure to add the following line to your ``project.py`` file:

.. code-block:: python

       import flow.environments

We develop environment profiles on a *per need* basis.
Please `contact us <mailto:signac-support@umich.edu>`_ if you have trouble running **signac-flow** on your local cluster or need assistance in writing a profile.
