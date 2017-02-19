Example Project
===============

Basics
------

The signac-flow package is designed to operate with a scheduler.
We submit operations to the scheduler by generating a script, which
contains the commands to execute these.

.. important::

    The following example is designed to teach how to use a FlowProject.
    To implement an actual project, it is usually better to fork the
    signac-project-template_ instead of starting from scratch.

.. _signac-project-template: https://github.com/glotzerlab/signac-project-template

For the sake of example, let's make the following assumptions:

  1. We operate on a signac data space spanned by parameters ``a`` and ``b``.
  2. We initialize the data space and each job in  a ``init.py`` script by initializing
     each job and creating an ``init.txt`` file.
  3. After initialization we want to execute a *job-operation* on a cluster using the ``foo``
     program which requires the name of the input file and a parameter ``a`` as arguments.
  4. The output of ``foo`` will be stored in a file called ``out.txt``.
  5. The ``foo`` program may be parallelized with MPI_ on up to ``b`` ranks.
  6. Finally, we want to label our jobs, so that we can assess the project's overall state.

.. _MPI: https://en.wikipedia.org/wiki/Message_Passing_Interface

Initialization
--------------

To get started we implement an initialization routine:

.. code-block:: python

    # init.py
    import signac

    project = signac.init_project('MyProject')

    for a in range(10):
        for b in range(10):
            with project.open_job(dict(a=a, b=b)) as job:
                with open('init.txt', 'w') as file:
                    # Some initialization routine

Workflow logic
--------------

Then we will implement the workflow logic in a module called ``project.py`` by specializing a :py:class:`~flow.FlowProject`:

.. code-block:: python

    # project.py
    from math import ceil

    from flow import FlowProject
    from flow import JobOperation


    class MyProject(FlowProject):

        # The classification of our job workspace is simple:
        def classify(self, job):
            if job.isfile('init.txt'):
                yield 'initialized'
            if job.isfile('out.txt'):
                yield 'processed'

        # There is only one operation in this project:
        def next_operation(self, job):
            labels = set(self.classify(job))
            # We can't run any operation if the job has not been properly initialized.
            if 'initialized' not in labels:
                return

            # If 'processed' is not among the labels, we return the command that
            # will execute the operation.
            if 'processed' not in labels:
                return JobOperation('process', job, 'foo -a {} init.txt > out.txt'.format(job.sp.a))

        # The FlowProject will automatically gather and bundle operations that are eligible
        # for execution. The user needs to specify how exactly these bundled operations
        # are executed on the cluster.
        def submit_user(self, env, _id, operations, **kwargs):
            # First we define a helper function to determine the # of processors
            # required for a specific operation

            def np(op):
                return op.job.sp.b if op.name == 'process' else 1

            # We use this function to determine the total number of required processors.
            np_total = sum(map(op, operations))

            # Then we calculate the # of required nodes based on the
            # processors per node(ppn) provided during submission.
            nn = ceil(np_total / ppn)

            # We start writing the job submission script by creating a JobScript instance
            # from the environment (env). The JobScript class is a thin io.StringIO wrapper,
            # essentially a in-memory text file.
            #
            # The JobScript instance contains a environment specific header and the exact
            # arguments required depend on the specific environment, but most environments
            # require a name (_id), the number of nodes (nn) and processors per node (ppn).
            sscript = env.script(_id, nn=nn, ppn=ppn)

            # Then we write the command for each operation to the script, assuming that we
            # want to execute the operation from within the job's workspace.
            #
            # The `write_cmd()` method will write the command to the script, wrapping it with
            # the environment specific mpi execution command if np is larger than one and making
            # sure that the command is executed in the background if bg is True.
            for op in operations:
                sscript.writeline('cd {}'.format(op.job.workspace()))
                sscript.write_cmd(op.cmd, np=np(op), bg=True)

            # Finally, we want to wait until all processes have finished before exiting the script.
            sscript.writeline('wait')

Workflow Execution
------------------

We can initialize the project's data space by executing the ``init.py`` script:

.. code-block:: bash

    $ python init.py

To print the status of the project to screen, execute:

.. code-block:: python

    >>> project = MyProject()
    >>> project.print_status(detailed=True, params=('a',))
    Status project 'test-project':
    Total # of jobs: 10
    label         progress
    ------------  --------------------------------------------------
    initialized   |########################################| 100.00%
    processed     |##########################--------------| 66.67%
    Detailed view:
    job_id                           S  next_job  a  labels
    -------------------------------- -  --------  -  -------------------------
    108ef78ec381244447a108f931fe80db U            1  initialized, processed
    be01a9fd6b3044cf12c4a83ee9612f84 U            2  initialized, processed
    32764c28ef130baefebeba76a158ac4e U  process   3  initialized
    # ...
    >>>

We can submit the operations to the cluster by executing the ``project.submit()`` function.
In many cases it is useful to create a separate ``submit.py`` script, which allows to submit
operations directly from the command line, for example like this:

.. code-block:: python

    # submit.py
    import argparse

    from flow import get_environment

    from project import MyProject

    project = MyProject.get_project()
    env = get_environment()

    parser = argparser.ArgumentParser()
    MyProject.add_submit_arguments(parser)
    args = parser.parse_args()

    project.submit(env, **vars(args))

This means we can execute the submission like this:

.. code-block:: bash

    $ python submit.py

To get help concerning possible command line options, use the ``--help`` argument:

.. code-block:: bash

   $ python submit.py --help
