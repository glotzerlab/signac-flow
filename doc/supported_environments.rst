.. _supported_environments:

======================
Supported Environments
======================

The **signac-flow** package streamlines the :ref:`submission <cluster-submission>` of job-operations to a supercomputer scheduling system.
The package currently ships with drivers for:

  * `SLURM schedulers <https://slurm.schedmd.com/>`_
  * `PBS (Torque) schedulers <http://www.adaptivecomputing.com/products/open-source/torque/>`_

Any supercomputing system utilizing these schedulers is supported out of the box.

In addition, the package provides specialized submission profiles for the following environments:

  * `Titan (Incite) <https://www.olcf.ornl.gov/titan/>`_
  * `Comet (XSEDE) <http://www.sdsc.edu/services/hpc/hpc_systems.html#comet>`_
  * `Bridges (XSEDE) <https://www.psc.edu/bridges>`_
  * `Stampede (XSEDE) <https://www.tacc.utexas.edu/systems/stampede2>`_
  * `flux (University of Michigan) <http://arc-ts.umich.edu/systems-services/flux/>`_

To use these environment profiles, make sure to add the following line to your ``project.py`` file:

.. code-block:: python

       import flow.environments

For more information, please see the :ref:`section about managing environments <environments>`.

We develop environment profiles on a *per need* basis.
Please `contact us <mailto:signac-support@umich.edu>`_ if you have trouble running **signac-flow** on your local cluster or need assistance in writing a profile.
