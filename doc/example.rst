Example Project
---------------

The signac-flow package is designed to operate with a scheduler.
We submit operations to the scheduler by generating a script, which
contains the commands to execute these.
To define an operation you need therefore express it as a command.

Let's assume the following example:

  1. We want to initialize a job, with a python function, which
     requires a parameter ``a`` and creates a file called ``init.txt``.
  2. After a job workspace is initialized, we want to operate on said file
     with a program called ``process``, which requires both parameters ``a`` and ``b``.
  3. The ``process`` program is capable to use MPI and may use up to ``b`` processors.
  4. Finally, we want to label our jobs, so that we can see the state of our project.

To get started, specialize a :py:class:`~flow.FlowProject`:

.. code-block:: python

    import os
    from flow import FlowProject

    class MyProject(FlowProject):

        # Let's switch into the project's root directory before anything else.
        def write_header(self, script):
            script.writeline('cd {}'.format(self.root_directory()))

        # All operations are defined in the write_user function:
        def write_user(self, script, job, operation, **kwargs):
            if operation == 'init':
                cmd = 'python scripts/init.py {}'.format(job)
                return script.write_cmd(cmd, **kwargs)
            elif operation == 'process':
                cmd = 'process -a {a} -b {b} {in} {out}'.format(
                  in=os.path.join(job.workspace(), 'init.txt'),
                  out=os.path.join(job.workspace(), 'out.txt'),
                  ** job.statepoint())
                return script.write_cmd(cmd, np=job.statepoint()['b'], **kwargs)
            else:
                raise RuntimeError("Unknown operation '{}'.".format(operation))

        # The classification of our job workspace is simple:
        def classify(self, job):
            if job.isfile('init.txt'):
                yield 'initialized'
            if job.isfile('out.txt'):
                yield 'processed'

        # We only want to execute the 'process' operation if the job is initialized:
        def eligible(self, job, operation):
            if operation == 'process':
                return 'initialized' in set(self.classify(job))
            else:
              return True

        def next_operation(self, job):
            labels = set(self.classify(job))
            if 'initialized' in labels and 'processed' not in labels:
                return 'process'

The ``init`` process executes the *scripts/init.py* script, let's implement this script:

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
    job_id                           a  status      next_job  labels
    -------------------------------- -  --------  ----------  ----------------------------------------------
    108ef78ec381244447a108f931fe80db 1  unknown               initialized, processed
    be01a9fd6b3044cf12c4a83ee9612f84 2  unknown               initialized, processed
    32764c28ef130baefebeba76a158ac4e 3  unknown   process     initialized
    # ...
    >>>

.. tip::

      Use a multiprocessing pool to speed up the status determination:

      .. code-block:: python

          with multiprocessing.Pool() as pool:
              project.print_status(detailed=True, params=('a',), pool=pool)

We will use the :py:class:`~flow.scheduler.FakeScheduler` for demonstration of the submit process, which simply prints the job scripts to screen:

.. code-block:: python

    >>> import flow
    >>> project = MyProject()
    >>> scheduler = flow.scheduler.FakeScheduler()
    >>> project.submit(scheduler, num=1)
    cd wait
    cd /project/root/path/test-flow-project

    process -a 0 -b 0 /project/workspace/in.txt /project/workspace/out.txt

    wait
    >>>
