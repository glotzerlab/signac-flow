.. _environments:

===================
Manage Environments
===================

The **signac-flow** package uses environment profiles to adjust the submission process to local environments.
That is because different environments provide different resources and options for the submission of operations to those resources.
The basic options will always be the same, however there might be some subtle differences depending on where you want to submit your operations.

.. tip::

    If you are running on a high-performance super computer, add the following line to your ``project.py`` module to import packaged profiles: ``import flow.environments``
    Please see :ref:`supported_environments` for more information.

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

Those are the steps usually required to define a new environment:

  1. Subclass from :py:class:`.ComputeEnvironment`.
  2. Determine a host name pattern that would match the output of :py:func:`socket.gethostname()`.
  3. Create a template and specify the template name as ``template`` class variable.

This is an example for a typical environment class definition:

.. code-block:: python

      class MyUniversityCluster(flow.DefaultTorqueEnvironment):

          hostname_pattern = 'mycluster.*.university.edu'
          template = 'mycluster.myuniversity.sh'

Then, add the ``mycluster.myuniversity.sh`` template script to the *templates/* directory within your project root directory.

.. important::

    The new environment will be automatically registered and used as long as it is either defined within the same module as your :py:class:`~.flow.FlowProject` class or imported into the same module.

As an example on how to write a submission script template, this is the default ``slurm.sh`` template for environments with SLURM scheduler included with the flow package:

.. literalinclude:: ../flow/templates/slurm.sh

All templates, which are shipped with the package, are within the *flow/templates/* directory within the package source code.


Contributing Environments to the Package
========================================

Users are **highly encouraged** to contribute environment profiles that they developed for their local environments.
In order to contribute an environment, either simply email them to the package maintainers (see the README for contact information) or create a pull request.
