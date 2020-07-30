.. _api:

API Reference
=============

This is the API for the **signac-flow** application.

Command Line Interface
----------------------

Some core **signac-flow** functions are---in addition to the Python interface---accessible
directly via the ``$ flow`` command.

For more information, please see ``$ flow --help``.

.. literalinclude:: cli-help.txt


The FlowProject
---------------

.. currentmodule:: flow

.. autoclass:: FlowProject

.. rubric:: Attributes

.. autosummary::

    FlowProject.ALIASES
    FlowProject.add_operation
    FlowProject.completed_operations
    FlowProject.export_job_statuses
    FlowProject.get_job_status
    FlowProject.label
    FlowProject.labels
    FlowProject.main
    FlowProject.make_group
    FlowProject.next_operations
    FlowProject.operation
    FlowProject.operations
    FlowProject.post
    FlowProject.post.copy_from
    FlowProject.post.false
    FlowProject.post.isfile
    FlowProject.post.never
    FlowProject.post.not_
    FlowProject.post.true
    FlowProject.pre
    FlowProject.pre.after
    FlowProject.pre.copy_from
    FlowProject.pre.false
    FlowProject.pre.isfile
    FlowProject.pre.never
    FlowProject.pre.not_
    FlowProject.pre.true
    FlowProject.run
    FlowProject.run_operations
    FlowProject.scheduler_jobs
    FlowProject.script
    FlowProject.submit
    FlowProject.submit_operations


.. autoclass:: FlowProject
    :show-inheritance:
    :members:
    :exclude-members: pre,post


.. automethod:: flow.FlowProject.post

.. automethod:: flow.FlowProject.post.copy_from

.. automethod:: flow.FlowProject.post.false

.. automethod:: flow.FlowProject.post.isfile

.. automethod:: flow.FlowProject.post.never

.. automethod:: flow.FlowProject.post.not_

.. automethod:: flow.FlowProject.post.true

.. automethod:: flow.FlowProject.pre

.. automethod:: flow.FlowProject.pre.after

.. automethod:: flow.FlowProject.pre.copy_from

.. automethod:: flow.FlowProject.pre.false

.. automethod:: flow.FlowProject.pre.isfile

.. automethod:: flow.FlowProject.pre.never

.. automethod:: flow.FlowProject.pre.not_

.. automethod:: flow.FlowProject.pre.true

.. autoclass:: flow.IgnoreConditions

.. autoclass:: flow.render_status.Renderer
    :members: generate_terminal_output, generate_html_output, render


@flow.cmd
---------

.. autofunction:: cmd

@flow.with_job
--------------

.. autofunction:: with_job

@flow.directives
----------------

.. autoclass:: directives

flow.run()
----------

.. autofunction:: run

flow.init()
-----------

.. autofunction:: init

flow.get_environment()
----------------------

.. autofunction:: get_environment

The FlowGroup
-------------

.. autoclass:: flow.project.FlowGroup
   :members: add_operation, complete, eligible, isdisjoint
