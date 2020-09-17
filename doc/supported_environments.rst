.. _supported-environments:

======================
Supported Environments
======================

The **signac-flow** package streamlines the submission of job operations to a supercomputer scheduling system.
A different template is used for each scheduler and system, but all templates extend from the :ref:`base-script`.

.. _base-script: supported_environments/base

.. toctree::
    :hidden:

    supported_environments/base

The package currently ships with scheduler support for:

.. toctree::

   supported_environments/slurm
   supported_environments/torque
   supported_environments/lsf

Any supercomputing system utilizing these schedulers is supported out of the box.
In addition, the package provides specialized submission templates for the following environments:

.. toctree::
   supported_environments/summit
   supported_environments/stampede2
   supported_environments/comet
   supported_environments/bridges
   supported_environments/umich
   supported_environments/umn

We develop environment templates and add scheduler support on an as-needed basis.
Please `contact us <mailto:signac-support@umich.edu>`_ if you have trouble running **signac-flow** on your local cluster or need assistance with creating a template or supporting a new scheduler.
