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

   supported_environments/lsf
   supported_environments/pbs
   supported_environments/slurm

Any supercomputing system utilizing these schedulers is supported out of the box.
In addition, the package provides specialized submission templates for the following environments:

.. toctree::
   supported_environments/summit
   supported_environments/andes
   supported_environments/crusher
   supported_environments/frontier
   supported_environments/anvil
   supported_environments/bridges2
   supported_environments/delta
   supported_environments/expanse
   supported_environments/stampede2
   supported_environments/drexel
   supported_environments/umich
   supported_environments/umn

We develop environment templates and add scheduler support on an as-needed basis.
Please `contact us <mailto:signac-support@umich.edu>`__ if you have trouble running **signac-flow** on your local cluster or need assistance with creating a template or supporting a new scheduler.
