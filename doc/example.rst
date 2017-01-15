Example Project
---------------

The signac-flow package is designed to operate with a scheduler.
We submit operations to the scheduler by generating a script, which
contains the commands to execute these.

Let's assume the following example:

  1. We operate on a signac data space spanned by parameters ``a`` and ``b``.
  2. We initialize each job with a python function script ``init.py`` which accepts the *job id* as argument and generates the input file ``init.txt``.
  3. The initialization routine is computationally cheap and should not be executed to the cluster.
  4. After initialization we want to execute a *job-operation* using the ``foo`` program which requires the name of the input and output files as argument.
  5. The ``foo`` program may be parallelized with MPI_ on ``b`` # of ranks.
  6. Finally, we want to label our jobs, so that we can assess the state of the project.

.. _MPI: https://en.wikipedia.org/wiki/Message_Passing_Interface

Each job script has 3 components:

  1. environment header
  2. project-specific header
  3. job-operation commands

The first header is used to setup the environment for different cluster environments.
The project-specific header contains command that we want to execute *once* for each submission, specifically related to this project.
The job-operation commands actually execute the job-operation.

We will assume that the environment headers are provided.
To get started, we crate a python module called `project.py` and specialize a :py:class:`~flow.FlowProject`:

.. code-block:: python

    # project.py

    from flow import FlowProject

    class MyProject(FlowProject):

        # For this project, we always want to switch into the project's root directory:
        def write_header(self, script):
            script.writeline('cd {}'.format(self.root_directory()))

        # All operations are defined in the write_user function:
        def write_user(self, script, job, operation, **kwargs):
            if operation == 'init':
                cmd = 'python init.py {}'.format(job)
                return script.write_cmd(cmd, **kwargs)
            elif operation == 'foo':
                script.writeline('cd {}'.format(job.workspace()))
                return script.write_cmd('foo -o out.txt init.txt', np=job.sp.b, **kwargs)
            else:
                raise RuntimeError("Unknown operation '{}'.".format(operation))

        # The classification of our job workspace is simple:
        def classify(self, job):
            if job.isfile('init.txt'):
                yield 'initialized'
            if job.isfile('out.txt'):
                yield 'processed'

        # There are only two operations in this project:
        def next_operation(self, job):
            labels = set(self.classify(job))
            if 'initialized' not in labels:
                return 'initialize'
            if 'processed' not in labels:
                return 'process'

        # Only the 'foo' operation should be submitted to a cluster
        def eligible(self, job, operation):
            if operation is not None and operation == 'foo':
                return True

Let's implement the ``init.py`` script:

.. code-block:: python

    # init.py
    import argparse
    import signac

    parser = argparse.ArgumentParser()
    parser.add_argumnet('jobid')
    args = parser.parse_args()

    project = signac.get_project()
    with project.open_job(id=args.jobid) as job:
        with open('init.txt', 'w') as file:
            # You're initialization routine...

Now we are ready to execute this workflow.

To print the status of your project:

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
