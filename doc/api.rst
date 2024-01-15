.. _api:

API Reference
=============

This is the API for the **signac-flow** application.

The FlowProject
---------------

.. currentmodule:: flow

.. rubric:: Attributes

.. autosummary::

    FlowProject.ALIASES
    FlowProject.completed_operations
    FlowProject.get_job_status
    FlowProject.label
    FlowProject.labels
    FlowProject.main
    FlowProject.make_group
    FlowProject.operation
    FlowProject.operation_hooks
    FlowProject.operation_hooks.on_exception
    FlowProject.operation_hooks.on_exit
    FlowProject.operation_hooks.on_start
    FlowProject.operation_hooks.on_success
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
    FlowProject.print_status
    FlowProject.project_hooks
    FlowProject.run
    FlowProject.scheduler_jobs
    FlowProject.submit


.. autoclass:: FlowProject
    :show-inheritance:
    :members:
    :exclude-members: pre,post,operation,operation_hooks

.. automethod:: flow.FlowProject.operation(func, name=None)

.. automethod:: flow.FlowProject.operation_hooks(hook_func, trigger)

.. automethod:: flow.FlowProject.operation_hooks.on_exception

.. automethod:: flow.FlowProject.operation_hooks.on_exit

.. automethod:: flow.FlowProject.operation_hooks.on_start

.. automethod:: flow.FlowProject.operation_hooks.on_success

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
    :members:

Operations and Status
---------------------

.. autoclass:: flow.project.BaseFlowOperation
    :members:

.. autoclass:: flow.project.FlowOperation
    :show-inheritance:
    :members:
    :special-members: __call__

.. autoclass:: flow.project.FlowCmdOperation
    :show-inheritance:
    :members:
    :special-members: __call__

Labels
------

.. autoclass:: flow.label
    :members:
    :special-members: __call__

.. autoclass:: flow.staticlabel
    :members:
    :special-members: __call__

.. autoclass:: flow.classlabel
    :members:
    :special-members: __call__

flow.init()
-----------

.. autofunction:: init

flow.get_environment()
----------------------

.. autofunction:: get_environment

The FlowGroup
-------------

.. autoclass:: flow.project.FlowGroup
    :members:

.. autoclass:: flow.project.FlowGroupEntry
    :members:
    :special-members: __call__

Aggregation
-----------

.. autoclass:: flow.aggregator
    :members:
    :special-members: __call__

.. autofunction:: flow.get_aggregate_id

Hooks
-----

.. autoclass:: flow.hooks.TrackOperations
   :members:

Compute Environments
--------------------

.. automodule:: flow.environment

.. autofunction:: flow.environment.setup

.. autofunction:: flow.environment.template_filter

.. autoclass:: flow.environment.ComputeEnvironment
    :members:

.. autoclass:: flow.environment.StandardEnvironment
    :members:

.. autoclass:: flow.environment.TestEnvironment
    :members:

.. autoclass:: flow.environment.SimpleSchedulerEnvironment
    :members:

.. autofunction:: flow.environment.registered_environments

Schedulers
----------

.. automodule:: flow.scheduling

.. autoclass:: flow.scheduling.base.JobStatus
    :members:
    :undoc-members:

.. autoclass:: flow.scheduling.base.ClusterJob
    :members:

.. autoclass:: flow.scheduling.base.Scheduler
    :members:

.. autoclass:: flow.scheduling.fake_scheduler.FakeScheduler
    :members:

.. autoclass:: flow.scheduling.lsf.LSFScheduler
    :members:

.. autoclass:: flow.scheduling.pbs.PBSScheduler
    :members:

.. autoclass:: flow.scheduling.simple_scheduler.SimpleScheduler
    :members:

.. autoclass:: flow.scheduling.slurm.SlurmScheduler
    :members:

Error Classes
-------------

.. automodule:: flow.errors
    :members:
