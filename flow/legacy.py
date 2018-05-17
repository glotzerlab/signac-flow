# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Wrapper functions to detect and support deprecated APIs from previous versions."""
import logging
import functools
import warnings


logger = logging.getLogger(__name__)


def support_submit_legacy_api(func):
    from inspect import isclass
    from .scheduling.base import Scheduler
    from .environment import ComputeEnvironment

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except TypeError as error:
            if str(error) != "submit() got multiple values for argument 'bundle_size'":
                raise
        except ValueError as error:
            if not str(error).startswith("Stop argument for islice() must be None or an integer"):
                raise

        env = args[0]
        args = tuple(args[1:])
        api_version = None
        if isclass(env):
            if issubclass(env, Scheduler):
                api_version = 3
            elif issubclass(env, ComputeEnvironment):
                if hasattr(self, 'submit_user'):
                    api_version = 4
                else:
                    api_version = 5
        elif any(key in kwargs for key in ('nn', 'np', 'ppn')):
            api_version = 5

        # Raise an exception or warn based on the detected API version.
        if api_version is None:
            raise RuntimeError("Unable to determine legacy API use.")
        elif api_version < 3:
            raise RuntimeError(
                "This FlowProject implementation uses deprecated API "
                "version 0.{}.x Please downgrade your signac-flow installation "
                "or update your project.".format(api_version))
        else:
            logger.warning(
                "Activating legacy layer for API version 0.{}.x.".format(api_version))

        # Delegate to legacy submit function based on detected API version.
        if api_version == 4:
            kwargs.setdefault('walltime', 12)
            if kwargs.get('nn') is not None:
                raise RuntimeError(
                    "You can't directly provide the number of nodes with the legacy API "
                    "for version 0.4.x!")
            return submit_04(self=self, env=env, *args, **kwargs)
        elif api_version == 5:
            if 'serial' in kwargs and 'parallel' not in kwargs:
                kwargs['parallel'] = not kwargs.pop('serial')
            return func(self, env=env, _api_version=5, *args, **kwargs)
        else:
            return func(self, env=env, *args, **kwargs)

    return wrapper


def submit_04(self, env, job_ids=None, operation_name=None, walltime=None,
              num=None, force=False, bundle_size=1, cmd=None, requires=None, **kwargs):
        """Submit job-operations to the scheduler.

        This method will submit an operation for each job to the environment's scheduler,
        unless the job is considered active, e.g., because an operation associated with
        the same job has alreay been submitted.

        The actual execution of operations is controlled in the :py:meth:`~.submit_user`
        method which must be implemented by the user.

        :param env: The env instance.
        :type env: :class:`~.flow.manage.ComputeEnvironment`
        :param job_ids: A list of job_id's, whose next operation shall be executed.
            Defaults to all jobs found in the workspace.
        :param operation_name: If not None, only execute operations with this name.
        :param walltime: The maximum wallclock time in hours.
        :type walltime: float
        :param num: If not None, limit number of submitted operations to `num`.
        :type num: int
        :param force: Ignore warnings and checks during submission, just submit.
        :type force: bool
        :param bundle_size: Bundle up to 'bundle_size' number of operations during submission.
        :type bundle_size: int
        :param cmd: Construct and submit an operation "on-the-fly" instead of submitting
            the "next operation".
        :type cmd: str
        :param requires: A job's set of classification labels must fully intersect with
            the labels provided as part of this argument to be considered for submission.
        :type requires: Iterable of str
        :param kwargs: Other keyword arguments which are forwarded to down-stream methods.
        """
        from .project import JobOperation, make_bundles
        from itertools import islice
        import datetime
        if walltime is not None:
            walltime = datetime.timedelta(hours=walltime)

        if job_ids:
            jobs = (self.open_job(id=_id) for _id in job_ids)
        else:
            jobs = iter(self)

        def get_op(job):
            if cmd is None:
                return self.next_operation(job)
            else:
                return JobOperation(name='user-cmd', cmd=cmd.format(job=job), job=job)

        def eligible(op):
            if force:
                return True
            if cmd is None:
                if operation_name is not None and op.name != operation_name:
                    return False
            if requires is not None:
                labels = set(self.classify(op.job))
                if not all([req in labels for req in requires]):
                    return False
            return self.eligible_for_submission(op)

        # Get the first num eligible operations
        operations = islice((op for op in map(get_op, jobs) if eligible(op)), num)

        # Bundle all eligible operations and submit the bundles
        for bundle in make_bundles(operations, bundle_size):
            _id = self._store_bundled(bundle)
            status = self.submit_user(
                env=env,
                _id=_id,
                operations=bundle,
                walltime=walltime,
                force=force,
                **kwargs)
            if status is not None:
                logger.info("Submitted job '{}' ({}).".format(_id, status.name))
                for op in bundle:
                    op.set_status(status)


def support_submit_operations_legacy_api(func):
    from .environment import ComputeEnvironment

    @functools.wraps(func)
    def wrapper(self, operations, _id=None, env=None, *args, **kwargs):
        if isinstance(operations, ComputeEnvironment) and isinstance(env, list):
            warnings.warn(
                "The FlowProject.submit_operations() signature has changed!", DeprecationWarning)
            env, operations = operations, env
        if kwargs.get('serial') is not None:
            warnings.warn(
                "The 'serial' argument for submit_operations() is deprecated and has been "
                "replaced by the 'parallel' argument as of version 0.7.", DeprecationWarning)
            kwargs['parallel'] = not kwargs.pop('serial')
        for key in 'nn', 'ppn':
            if key in kwargs:
                warnings.warn(
                    "The '{}' argument should be provided as part of the operation "
                    "directives as of version 0.7.".format(key), DeprecationWarning)
                for op in operations:
                    assert op.directives.setdefault(key, kwargs[key]) == kwargs[key]
        return func(self, operations=operations, _id=_id, env=env, *args, **kwargs)

    return wrapper


def support_run_legacy_api(func):

    @functools.wraps(func)
    def wrapper(self, jobs=None, names=None, *args, **kwargs):
        from .project import JobOperation
        legacy = 'operations' in kwargs
        if not legacy and jobs is not None:
            jobs = list(jobs)
            for job in jobs:
                if isinstance(job, JobOperation):
                    legacy = True
                break

        if legacy:
            logger.warning(
                "The FlowProject.run() function has been renamed to run_operations() "
                "as of version 0.6. Using legacy compatibility layer.")
            return self.run_operations(*args, **kwargs)
        else:
            return func(self, jobs=jobs, names=names, *args, **kwargs)

    return wrapper
