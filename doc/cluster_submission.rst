.. _cluster-submission:

==================
Cluster Submission
==================

Using **signac-flow** to manage the submission of operations to a cluster environment, enables us to **keep track** of the status of individual operations, such as whether they are queued or running.
The following sections describe briefly how to setup a submission process.

The *submit* interface
======================

In general, we submit operations through the primary interface of the :py:class:`~.FlowProject`, e.g. by setting up a ``project.py`` module, like this:

.. code-block:: python

    # project.py
    from flow import FlowProject

    class Project(FlowProject):
        pass

    if __name__ == '__main__':
        Project().main()

We can then submit operation from the command line with the following command:

.. code-block:: bash

      $ python project.py submit

.. tip::

    Use the ``script`` command to debug the generation of execution scripts before generating submission scripts.

In general, the ``submit`` interface options will depend on your local environment.
For example, the options available to you will be different when running this on your local laptop compared to a high-performance super computer.

.. note::

    Unless you have one of the supported schedulers installed, you will not be able to submit any operations on your local laptop, however you will be able to run some test commands in order to debug the process as best as you can.
    On the other hand, if you are in one of the natively supported high-performance super computing environments (e.g. XSEDE), you may take advantage of configurations profiles specifically taylored to those environments.

Submitting Operations
=====================

The submission process consists of the following steps:

  1. Gathering of all operations eligible for submission.
  2. Generating of scripts to execute those operations.
  3. Submission of those scripts to the scheduler.

The first step is largely determined by your project *workflow*.
You can see which operation might be submitted by looking at the output of ``$ python project.py status --detailed``.
You may further reduce the operations to be submitted by selecting specifc job (``-j``), specific operations (``-o``) or generally reduce the total number of operations to be submitted (``-n``).
For example the following command would submit a total number of 5 ``hello`` operations:

.. code-block:: bash

    $ python project.py submit -o hello -n 5


Operation Bundling
==================

By default all operations will be submitted as a seperate cluster job.
However, you may choose to bundle multiple operations into one submission using the ``--bundle`` option, e.g., if you need to run multiple processes in parallel to fully utilize one node.

If you have many small operations, you can *bundle* them and run them in *serial* using the ``--serial`` option.


Parallelization
===============

When submitting operations to the cluster, **signac-flow** assumes that each operations requires one processor and will generate a script requesting the resources accordingly.

When you execute *parallelized* operations you need to specify that with your *operation*.
For example, assuming that we a program called ``foo``, which will automatically parallelize onto 24, cores we could need to specify this for our operation like that:

.. code-block:: python

    class MyProject(FlowProject):

        def __init__(self, *args, **kwargs):
            super(MyProject, self).__init__(*args, **kwargs)
                self.add_operation(
                  name='foo',                         # name of the operation
                  cmd='cd {job.ws}; foo input.txt',   # the execution command
                  np=24,                              # foo requires 24 cores
                )

If you are using MPI for parallelization, you may need to prefix your command:

.. code-block:: python

    cmd='cd {job.ws}; mpirun -np 24 foo input.txt'

Different environment use different MPI-commands, you can use your environment-specific MPI-command like that:

.. code-block:: python

    from flow import get_environment

    # ..
        env = get_environment()

        self.add_operation(
          name='foo',
          cmd='cd {job.ws};' +  env.mpi_cmd('foo input.txt', np=24),
          np=24,
        )

.. tip::

    Both the ``cmd``-argument and the ``np``-argument may be *callables*, that means you can specify both the command itself, but also the number of processors **as a function of job**!

Managing Environments
=====================

The **signac-flow** package attempts to detect your local environment and based on that adjust the options provided by the ``submit`` interface.
You can check which environment you are using, by looking at the output of ``submit --help``.

The :py:func:`~.get_environment` function will go through all defined :py:class:`~.ComputeEnvironment` classes and return the one, where the :py:meth:`~.ComputeEnvironment.is_present` class method returns ``True``.

To use an environment, you need to define or import it prior to calling the ``submit`` interface.
This means in practice, that you will need to either define them directly or import them within your ``project.py`` module.

.. tip::

    If you are running on a high-performance super computer, add the following line to your ``project.py`` module to import default profiles: ``import flow.environments``
