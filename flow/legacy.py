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
        elif api_version < 5:
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
        if (
            cmd is None
            and operation_name is not None
            and op.name != operation_name
        ):
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


def support_print_status_legacy_api(func):
    from .scheduling.base import Scheduler

    @functools.wraps(func)
    def wrapper(self, jobs=None, *args, **kwargs):
        print('args', args)
        print('kwargs', kwargs)
        for deprecated_arg in ('scheduler', 'pool', 'job_filter'):
            if deprecated_arg in kwargs:
                raise RuntimeError(
                    "The {} argument for flow.FlowProject.print_status() "
                    " was deprecated as of signac-flow version 0.6 and has "
                    "been removed as of version 0.7!".format(deprecated_arg))
        if isinstance(jobs, Scheduler):
            raise ValueError(
                "The first argument of flow.FlowProject.print_status() is 'jobs' "
                "as of signac-flow version 0.6!")

        return func(self, jobs=jobs, *args, **kwargs)
    return wrapper


class JobsCursorWrapper(object):
    """Enables the execution of workflows on dynamic data spaces.

    Instead of storing a static list of jobs, we store the (optional)
    filters and evaluate them on each iteration.

    Note: This is the default behavior in upstream versions of signac core.
    """
    def __init__(self, project, filter=None, doc_filter=None):
        self._project = project
        self._filter = filter
        self._doc_filter = doc_filter

    def _find_ids(self):
        return self._project.find_job_ids(self._filter, self._doc_filter)

    def __iter__(self):
        return iter([self._project.open_job(id=_id) for _id in self._find_ids()])

    def __len__(self):
        return len(self._find_ids())


# ## Legacy templating system:


# Global variable that is used internally to keep track of which
# FlowProject methods belong to the legacy templating system. Such
# a method is docorated with the _part_of_legacy_template_system()
# decorator and then registered in this variable.
_LEGACY_TEMPLATING_METHODS = set()


def _part_of_legacy_template_system(method):
    "Label a method to be part of the legacy templating system."
    _LEGACY_TEMPLATING_METHODS.add(method.__name__)
    method._legacy_intact = True
    return method


class FlowProjectLegacyTemplatingSystem(object):

    def __init__(self, *args, **kwargs):
        super(FlowProjectLegacyTemplatingSystem, self).__init__(*args, **kwargs)
        self._setup_legacy_templating()

    def _setup_legacy_templating(self):
        """This function identifies whether a subclass has implemented deprecated template
        functions.

        The legacy templating system is used to generate run and cluster submission scripts
        if that is the case. A warning is emitted to inform the user that they will not be
        able to use the standard templating system.

        The legacy templating functions are decorated with the _part_of_legacy_template_system()
        decorator.
        """
        self._legacy_templating = False
        legacy_methods = set()
        for method in _LEGACY_TEMPLATING_METHODS:
            if hasattr(self, method) and not hasattr(getattr(self, method), '_legacy_intact'):
                warnings.warn(
                    "The use of FlowProject method '{}' is deprecated!".format(method),
                    DeprecationWarning)
                legacy_methods.add(method)
        if legacy_methods:
            self._legacy_templating = True
            warnings.warn(
                "You are using the following deprecated templating methods: {}. Please remove "
                "those methods from your project class implementation to use the jinja2 templating "
                "system (version >= 0.6).".format(', '.join(legacy_methods)))

    @_part_of_legacy_template_system
    def write_script_header(self, script, **kwargs):
        """"Write the script header for the execution script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        # Add some whitespace
        script.writeline()
        # Don't use uninitialized environment variables.
        script.writeline('set -u')
        # Exit on errors.
        script.writeline('set -e')
        # Switch into the project root directory
        script.writeline('cd {}'.format(self.root_directory()))
        script.writeline()

    @_part_of_legacy_template_system
    def write_script_operations(self, script, operations, background=False, **kwargs):
        """Write the commands for the execution of operations as part of a script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        from .util.misc import write_human_readable_statepoint

        for op in operations:
            write_human_readable_statepoint(script, op.job)
            script.write_cmd(op.cmd.format(job=op.job), bg=background)
            script.writeline()

    @classmethod
    @_part_of_legacy_template_system
    def write_human_readable_statepoint(cls, script, job):
        """Write statepoint of job in human-readable format to script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        raise RuntimeError(
            "The write_human_readable_statepoint() function has been deprecated "
            "as of version 0.6 and been removed as of version 0.7.")

    @_part_of_legacy_template_system
    def write_script_footer(self, script, **kwargs):
        """"Write the script footer for the execution script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        # Wait until all processes have finished
        script.writeline('wait')

    @_part_of_legacy_template_system
    def write_script(self, script, operations, background=False, **kwargs):
        """Write a script for the execution of operations.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.

        By default, this function will generate a script with the following components:

        .. code-block:: python

            write_script_header(script)
            write_script_operations(script, operations, background=background)
            write_script_footer(script)

        :param script:
            The script to write the commands to.
        :param operations:
            The operations to be written to the script.
        :type operations:
            A sequence of JobOperation
        :param background:
            Whether operations should be executed in the background;
            useful to parallelize execution.
        :type background:
            bool
        """
        self.write_script_header(script, **kwargs)
        self.write_script_operations(script, operations, background=background, **kwargs)
        self.write_script_footer(script, **kwargs)


def script_support_legacy_templating_system(func):

    @functools.wraps(func)
    def wrapper(self, operations, parallel=False, template='script.sh', *args, **kwargs):
        if self._legacy_templating:
            import os
            from .environment import TestEnvironment
            # We first check whether it appears that the user has provided a templating script
            # in which case we raise an exception to avoid highly unexpected behavior.
            fn_template = os.path.join(self.root_directory(), 'templates', template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))
            script = TestEnvironment.script()
            self.write_script(script, operations, background=parallel)
            script.seek(0)
            return script.read()
        else:
            return func(self, operations=operations, parallel=parallel, template=template,
                        *args, **kwargs)

    return wrapper


def _generate_submit_script_support_legacy_templating_system(func):

    @functools.wraps(func)
    def wrapper(self, _id, operations, template, show_template_help, env, *args, **kwargs):
        import os
        if self._legacy_templating:
            fn_template = os.path.join(self._template_dir, template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))

            # For maintaining backwards compatibility for Versions<0.7
            if kwargs['parallel']:
                np_total = sum(op.directives['np'] for op in operations)
            else:
                np_total = max(op.directives['np'] for op in operations)

            script = env.script(_id=_id, np_total=np_total, **kwargs)
            background = kwargs.pop('parallel', not kwargs.pop('serial', False))
            self.write_script(script=script, operations=operations, background=background, **kwargs)
            script.seek(0)
            return script.read()
        else:
            return func(self, _id, operations, template, show_template_help, env, *args, **kwargs)

    return wrapper
