.. _cluster-submission:

==================
Cluster Submission
==================

While it is always possible to manually submit scripts like the one shown in the :ref:`previous section <project-script>` to a cluster, using the *flow interface* will allows us to **keep track of submitted operations** for example to prevent the resubmission of active operations.

In addition, **signac-flow** uses *environment profiles* to select which **base template** to use for the cluster job script generation.
All base templates are in essence the same, but will be slightly adapted to the current cluster environment.
That is because different cluster environments offer different resources and therefore require slightly different options for submission.
You can check out the available options with the ``python project.py submit --help`` command.

The *submit* interface
======================

In general, we submit operations through the primary interface of the :py:class:`~.FlowProject`.
We assume that we use the same ``project.py`` module as shown in the previous section.

Then we can submit operations from the command line with the following command:

.. code-block:: bash

      ~/my_project $ python project.py submit

This will submit all *eligible* job-operations to the cluster scheduler and block that specific job-operation from resubmission.

In some cases you can provide additional arguments to the scheduler, such as which partition to submit to, which will then be used by the template script.
In addition you can always forward any arguments directly to the scheduler as positional arguments.
For example, if we wanted to specify an account name with a *torque* scheduler, we could use the following command:

.. code-block:: bash

      ~/my_project $ python project.py submit -- -l A:my_account_name

Everything after the two dashes ``--`` will not be interpreted by the *submit* interface, but directly forwarded to the scheduler *as is*.

Unless you have one of the :ref:`supported schedulers <environments>` installed, you will not be able to submit any operations on your computer.
However, *signac-flow* comes with a simple scheduler for testing purposes.
You can execute it with ``$ simple-scheduler run`` and then follow the instructions on screen.

Submitting specific Operations
==============================

The submission process consists of the following steps:

  1. *Gathering* of all job-operations *eligible* for submission.
  2. Generation of scripts to execute those job-operations.
  3. Submission of those scripts to the scheduler.

The first step is largely determined by your project *workflow*.
You can see which operation might be submitted by looking at the output of ``$ python project.py status --detailed``.
You may further reduce the operations to be submitted by selecting specific jobs (*e.g.* with the ``-j`` or the ``-f`` options), specific operations (``-o``), or generally reduce the total number of operations to be submitted (``-n``).
For example the following command would submit up to 5 ``hello`` operations, where *a is less than 5*.

.. code-block:: bash

    ~/my_project $ python project.py submit -o hello -n 5 -f a.\$lt 5

The submission scripts are generated using the same templating system like the ``script`` command.

.. tip::

    Use the ``--pretend`` option to pre-view the generated submission scripts on screen instead of submitting them.


Parallelization and Bundling
============================

By default all eligible job-operations will be submitted as separate cluster jobs.
This is usually the best model for clusters that provide shared compute partitions.
However, sometimes it is beneficial to execute multiple operations within one cluster job, especially if the compute cluster can only make reservation for full nodes.

You can place multiple job-operations within one cluster submission with the ``--bundling`` option.
For example, the following command will bundle up to 5 job-operations to be executed in parallel into a single cluster submission:

.. code-block:: bash

    ~/my_project $ python project.py submit --bundle=5 --parallel

Without any argument the ``--bundle`` option will bundle **all** eligible job-operations into a single cluster job.

.. tip::

    Recognizing that ``--bundle=1`` is the default option might help you to better understand the bundling concept.

For more information on managing different environments, see the :ref:`next section <environments>`.
