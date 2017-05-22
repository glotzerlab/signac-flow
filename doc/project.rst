.. _flow-project:

===============
The FlowProject
===============

This chapter describes how to setup a complete workflow via the implementation of a :py:class:`~.FlowProject`.

.. _project-setup:

Setup and Interface
===================

To implement a more automated workflow, we can subclass a :py:class:`~.FlowProject`:

.. code-block:: python

    # project.py
    from flow import FlowProject

    class MyProject(FlowProject):
        pass

    if __name__ == '__main__':
        MyProject().main()

.. tip::

    You can generate boiler-plate templates like the one above with the ``$ flow init`` function.
    There are multiple different templates available via the ``-t/--template`` option.

Executing this script on the command line will give us access to this project's specific command line interface:

.. code-block:: bash

    $ python project.py
    usage: project.py [-h] {status,next,run,script,submit} ...

.. raw:: html

    <script type="text/javascript" src="https://asciinema.org/a/1t8omjgtokjpnto0gnqnz5hoq.js" id="asciicast-1t8omjgtokjpnto0gnqnz5hoq" async></script>

.. note::

    You can have **multiple** implementations of :py:class:`~.FlowProject` that all operate on the same **signac** project!
    This may be useful, for example, if you want to implement two very distinct workflows that operate on the same data space.
    Simply put those in different modules, *e.g.*, ``project_a.py`` and ``project_b.py``.


.. _classification:

Classification
==============

The :py:class:`~.FlowProject` uses a :py:meth:`~.FlowProject.classify` method to generate *labels* for a job.
A label is a short text string, that essentially represents a condition.
Following last chapter's example, we could implement a ``greeted`` label like this:

.. code-block:: python

    # project.py
    from flow import FlowProject
    from flow import staticlabel

    class MyProject(FlowProject):

        @staticlabel()
        def greeted(job):
            return job.isfile('hello.txt')
    # ...

Using the :py:class:`~.project.staticlabel` decorator turns the ``greeted()`` function into a function, which will be evaluated for our classification.
We can check that by executing the ``hello`` operation for a few job and then looking at the project's status:

.. code-block:: bash

    $ python operations.py hello 0d32 2e6
    hello 0d32543f785d3459f27b8746f2053824
    hello 2e6ba580a9975cf0c01cb3c3f373a412
    $ python project.py status --detailed
    Status project 'MyProject':
    Total # of jobs: 10

    label    progress
    -------  ----------
    [no labels]

    Detailed view:
    job_id                            S      next_op  labels
    --------------------------------  ---  ---------  --------
    0d32543f785d3459f27b8746f2053824  U               greeted
    14fb5d016557165019abaac200785048  U
    2af7905ebe91ada597a8d4bb91a1c0fc  U
    2e6ba580a9975cf0c01cb3c3f373a412  U               greeted
    42b7b4f2921788ea14dac5566e6f06d0  U
    751c7156cca734e22d1c70e5d3c5a27f  U
    81ee11f5f9eb97a84b6fc934d4335d3d  U
    9bfd29df07674bc4aa960cf661b5acd2  U
    9f8a8e5ba8c70c774d410a9107e2a32b  U
    b1d43cd340a6b095b41ad645446b6800  U

    Abbreviations used:
    S: status
    U: unknown

.. note::

    The status overview will show ``[no labels]`` -- even if you have implemented label-functions -- as long as none of the label functions actually returns ``True``.

.. raw:: html

    <script type="text/javascript" src="https://asciinema.org/a/48bs64h7cdo7mncnkk88ilrzm.js" id="asciicast-48bs64h7cdo7mncnkk88ilrzm" async></script>

.. _next-operation:

Determine the **next-operation**
================================

Next, we should tell the project, that the ``hello()`` operation is to be executed, whenever the ``greeted`` condition is **not met**.
We achieve this by adding the operation to the project:

.. code-block:: python

      class MyProject(FlowProject):

        def __init__(self, *args, **kwargs):
            super(MyProject, self).__init__(*args, **kwargs)

            self.add_operation(
              name='hello',
              cmd='python operations.py hello {job._id}',
              post=[MyProject.greeted])

Let's go through the individual arguments of the :py:meth:`~.FlowProject.add_operation` method:

The ``name`` argument is arbitrary, but must be unique for all operations part of the project's workflow.
It simply helps us to identify the operation without needing to look at the full command.

The ``cmd`` argument actually determines how to execute the particular operation, ideally it should be a function of job.
We can construct the ``cmd`` either by using formatting fields, as shown above.
We can use any attribute of our job instance, that includes state points (e.g. ``job.sp.a``) or the workspace directory (``job.ws``).
The command is later evaluated like this: ``cmd.format(job=job)``.

Alternatively, we can define a function that returns a command or script, e.g.:

.. code-block:: python

    # ...
        self.add_operation(
            name='hello',
            cmd=lambda job: "python operations.py hello {}".format(job),
            post=[MyProject.greeted])

Finally, the ``post`` argument is a list of unary condition functions.

.. admonition:: Definition:
    :class: note

    A specific operation is **eligible for execution**, whenever all pre-conditions (``pre``) are met and at least one of the post-conditions (``post``) is not met.

In this case, the ``hello`` operation will only be executed, when ``greeted()`` returns ``False``; we can check that again by looking at the status:

.. code-block:: bash

    $ python project.py status --detailed
    Status project 'MyProject':
    Total # of jobs: 10

    label    progress
    -------  -------------------------------------------------
    greeted  |########--------------------------------| 20.00%

    Detailed view:
    job_id                            S    next_op    labels
    --------------------------------  ---  ---------  --------
    0d32543f785d3459f27b8746f2053824  U               greeted
    14fb5d016557165019abaac200785048  U !  hello
    2af7905ebe91ada597a8d4bb91a1c0fc  U !  hello
    2e6ba580a9975cf0c01cb3c3f373a412  U               greeted
    42b7b4f2921788ea14dac5566e6f06d0  U !  hello
    751c7156cca734e22d1c70e5d3c5a27f  U !  hello
    81ee11f5f9eb97a84b6fc934d4335d3d  U !  hello
    9bfd29df07674bc4aa960cf661b5acd2  U !  hello
    9f8a8e5ba8c70c774d410a9107e2a32b  U !  hello
    b1d43cd340a6b095b41ad645446b6800  U !  hello

    Abbreviations used:
    !: requires_attention
    S: status
    U: unknown

.. raw:: html

    <script type="text/javascript" src="https://asciinema.org/a/cfx50fgliekgzu8xt7r79s5n7.js" id="asciicast-cfx50fgliekgzu8xt7r79s5n7" async></script>

.. _project-run:

Running project operations
==========================

Similar to the :py:func:`~.run` interface earlier, we can execute all pending operations with the ``python project.py run`` command:

.. code-block:: bash

     $ python project.py run
     hello 42b7b4f2921788ea14dac5566e6f06d0
     hello 2af7905ebe91ada597a8d4bb91a1c0fc
     hello 14fb5d016557165019abaac200785048
     hello 751c7156cca734e22d1c70e5d3c5a27f
     hello 9bfd29df07674bc4aa960cf661b5acd2
     hello 81ee11f5f9eb97a84b6fc934d4335d3d
     hello 9f8a8e5ba8c70c774d410a9107e2a32b
     hello b1d43cd340a6b095b41ad645446b6800

Again, the execution is automatically parallelized.

Let's remove a few random ``hello.txt`` files to regain pending operations:

.. code-block:: bash

    $ rm workspace/2af7905ebe91ada597a8d4bb91a1c0fc/hello.txt
    $ rm workspace/9bfd29df07674bc4aa960cf661b5acd2/hello.txt

.. raw:: html

    <script type="text/javascript" src="https://asciinema.org/a/2gfl9hfbveyu7583j338x6day.js" id="asciicast-2gfl9hfbveyu7583j338x6day" async></script>

.. _project-script:

Generating Execution Scripts:
=============================

Using the ``script`` command, we can generate an **operation** execution script based on the pending operations, which might look like this:

.. code-block:: bash

    $ python project.py script
    ---- BEGIN SCRIPT ----

    set -u
    set -e
    cd /Users/johndoe/my_project

    # Statepoint:
    #
    # {{
    #   "a": 4
    # }}
    python operations.py hello 2af7905ebe91ada597a8d4bb91a1c0fc &

    wait
    ---- END SCRIPT ----


    ---- BEGIN SCRIPT ----

    set -u
    set -e
    cd /Users/johndoe/my_project

    # Statepoint:
    #
    # {{
    #   "a": 0
    # }}
    python operations.py hello 9bfd29df07674bc4aa960cf661b5acd2 &

    wait
    ---- END SCRIPT ----

These scripts can be used for the execution of operations directly, or they could be submitted to a cluster environment for remote execution.
This brings us to the final chapter.

.. raw:: html

    <script type="text/javascript" src="https://asciinema.org/a/4jwqh0azk01rkterytxvzvr7g.js" id="asciicast-4jwqh0azk01rkterytxvzvr7g" async></script>


Cluster submission
==================

While it is always possible to submit scripts like the one shown in the previous section manually to a cluster, the advantage of using the flow interface is, that flow will be able to **keep track of submitted jobs** and for example prevent the resubmission of active operations.

The signac-flow submit interface will be adjusted based on the environment it is executed in.
For example, submitting to a torque scheduler might be a different compared to submitting to a slurm scheduler.
The basic options will be as similar as possible, however there might be slight subtleties that cannot all be covered here.

You can check out the options available to you using the ``python project.py submit --help`` command.
For more information, please see the :ref:`cluster-submission` chapter.

.. _flow-project-demo:

Full Demonstration
==================

The screencast below is a complete demonstration of all steps:

.. raw:: html

    <script type="text/javascript" src="https://asciinema.org/a/6uyqoqk87w1r5y0k09zj43ibp.js" id="asciicast-6uyqoqk87w1r5y0k09zj43ibp" async></script>
