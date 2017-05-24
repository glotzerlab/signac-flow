.. _environments:

===================
Manage Environments
===================

The **signac-flow** package uses environment profiles to adjust the submission process to local environments.
That is because different environments provide different resources and options for the submission of operations to those resources.
The basic options will always be the same, however there might be some subtle differences depending on where you want to submit your operations.

.. tip::

    If you are running on a high-performance super computer, add the following line to your ``project.py`` module to import packaged profiles: ``import flow.environments``

How to Use Environments
=======================

Environments are defined by subclassing from the :py:class:`~.ComputeEnvironment` class.
The :py:class:`~.ComputeEnvironment` class is a *meta-class* that is automatically globally registered whenever you define one.

This enables us to use environments simply by defining them or importing them from a different module.
The :py:func:`.get_environment` function will go through all defined :py:class:`~.ComputeEnvironment` classes and return the one, where the :py:meth:`~.ComputeEnvironment.is_present` class method returns ``True``.

Default Environments
====================

The package comes with a few *default environments* which are **always available**.
That includes the :py:class:`~.DefaultTorqueEnvironment` and the :py:class:`~.DefaultSlurmEnvironment`.
This means that if you are within an environment with a *torque* or *slurm scheduler* you should be immediately able to submit to the cluster.

There is also a :py:class:`~.environment.TestEnvironment`, which you can use by calling the :py:func:`.get_environment` function with ``test=True`` or by using the ``--test`` argument on the command line.

Packaged Environments
=====================

In addition, **signac-flow** comes with some additional *packaged environments*.
These environments are defined within the :py:mod:`flow.environments` module.
These environments are not automatically available, instead you need to *explictly import* the :py:mod:`flow.environments` module.

For a full list of all packaged environments, please see :ref:`packaged-environments`.

Defining New Environments
=========================

In order to implement a new environment, create a new class that inherits from :py:class:`.ComputeEnvironment`.
You will need to define a detection algorithm for your environment, be default we use a regular expression that matches the return value of ``socket.gethostname()``.

Those are ususally the steps we need to take:

  1. Subclass from :py:class:`.ComputeEnvironment`.
  2. Determine a host name pattern that would match the output of :py:func:`socket.gethostname()`.
  3. Optionally specify the ``cores_per_node`` for environments with compute nodes.
  4. Optionally overload the ``mpi_cmd()`` classmethod.
  5. Overload the ``script()`` method to add specific options to the header of the submission script.

This is an example for a typical environment class definition:

.. code-block:: python

      class MyUniversityCluster(flow.TorqueEnvironment):

          hostname_pattern = 'mycluster.*.university.edu'
          cores_per_node = 32

          @classmethod
          def mpi_cmd(cls, cmd, np):
              return 'mpirun -np {np} {cmd}'.format(n=np, cmd=cmd)

          @classmethod
          def script(cls, _id, **kwargs):
              js = super(MyUniversityCluster, cls).script(_id=_id, **kwargs)
              js.writeline("$PBS -A {}".format(cls.get_config_value('account'))
              return js

The :py:meth:`~.ComputeEnvironment.get_config_value` method allows us to get information from **signac**'s configuration which would be different for different users.
Unless you provide a default value as the second argument, the user will be prompted to add the requested value to their configuration when using this specific profile for the first time.

Contributing Environments to the Package
========================================

Users are **highly encouraged** to contribute environment profiles that they developed for their local environments.
In order to contribute an environment, either simply email them to the package maintainers (see the README for contact information), or add your environment directly to the ``flow.environments/__init__.py`` module and create a pull request!
