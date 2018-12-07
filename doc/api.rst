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
    FlowProject.NAMES
    FlowProject.OPERATION_STATUS_SYMBOLS
    FlowProject.PRETTY_OPERATION_STATUS_SYMBOLS
    FlowProject.add_operation
    FlowProject.classify
    FlowProject.completed_operations
    FlowProject.eligible_for_submission
    FlowProject.export_job_stati
    FlowProject.get_job_status
    FlowProject.label
    FlowProject.labels
    FlowProject.main
    FlowProject.map_scheduler_jobs
    FlowProject.next_operation
    FlowProject.next_operations
    FlowProject.operation
    FlowProject.operations
    FlowProject.post
    FlowProject.pre
    FlowProject.run
    FlowProject.run_operations
    FlowProject.scheduler_jobs
    FlowProject.script
    FlowProject.submit
    FlowProject.submit_operations
    FlowProject.update_aliases


.. autoclass:: FlowProject
    :show-inheritance:
    :members:
    :undoc-members: pre,post


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
