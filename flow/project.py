# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Workflow definition with the FlowProject.

The FlowProject is a signac Project, that allows the user to define
a workflow based on job classification and job operations.

A job may be classified based on its metadata and data in the form
of str labels. These str-labels are yielded in the classify() method.

Based on the classification a "next operation" may be identified, that
should be executed next to further the workflow. While the user is free
to choose any method for the determination of the "next operation", one
option is to use a FlowGraph.
"""
from __future__ import print_function
import sys
import os
import io
import logging
import warnings
import datetime
import json
import errno
from math import ceil
from collections import defaultdict
from itertools import islice
from hashlib import sha1

import signac
from signac.common.six import with_metaclass

from . import manage
from . import util
from .util.tqdm import tqdm


logger = logging.getLogger(__name__)

DEFAULT_WALLTIME_HRS = 12


def _mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as error:
        if not (error.errno == errno.EEXIST and os.path.isdir(path)):
            raise


def is_active(status):
    """True if a specific status is considered 'active'.

    A active status usually means that no further operation should
    be executed at the same time to prevent race conditions and other
    related issues.
    """
    for gid, s in status.items():
        if s > manage.JobStatus.inactive:
            return True
    return False


def draw_progressbar(value, total, width=40):
    "Helper function for the visualization of progress."
    n = int(value / total * width)
    return '|' + ''.join(['#'] * n) + ''.join(['-'] * (width - n)) + '|'


def abbreviate(x, a):
    "Abbreviate x with a and add to the abbreviation table."
    if x == a:
        return x
    else:
        abbreviate.table[a] = x
        return a
abbreviate.table = dict()  # noqa


def shorten(x, max_length=None):
    "Shorten x to max_length and add to abbreviation table."
    if max_length is None:
        return x
    else:
        return abbreviate(x, x[:max_length])


def _update_status(args):
    return manage.update_status(* args)


class label(object):
    """Decorate a function to be a label function.

    The label() method as part of FlowProject iterates over all
    methods decorated with this label and yields the method's name
    or the provided name.

    For example:

    .. code::

        class MyProject(FlowProject):

            @label()
            def foo(self, job):
                return True

            @label()
            def bar(self, job):
                return 'a' in job.statepoint()

        >>> for label in MyProject().labels(job):
        ...     print(label)

    The code segment above will always print the label 'foo',
    but the label 'bar' only if 'a' is part of a job's state point.

    This enables the user to quickly write classification functions
    and use them for labeling, for example in the classify() method.
    """

    def __init__(self, name=None):
        self.name = name

    def __call__(self, func):
        func._label = True
        if self.name is not None:
            func._label_name = self.name
        return func


class staticlabel(label):
    """A label decorator for staticmethods.

    This decorator implies "staticmethod"!
    """

    def __call__(self, func):
        return staticmethod(super(staticlabel, self).__call__(func))


class classlabel(label):
    """A label decorator for classmethods.

    This decorator implies "classmethod"!
    """

    def __call__(self, func):
        return classmethod(super(classlabel, self).__call__(func))


def _is_label_func(func):
    return getattr(getattr(func, '__func__', func), '_label', False)


def make_bundles(operations, size=None):
    """Utility function for the generation of bundles.

    This function splits a iterable of operations into
    equally sized bundles and a possibly smaller final
    bundle.
    """
    n = None if size == 0 else size
    while True:
        b = list(islice(operations, n))
        if b:
            yield b
        else:
            break


class JobOperation(object):
    """Define operations to apply to a job.

    A operation function in the context of signac is a function, with only
    one job argument. This in principle ensures that operations are deterministic
    in the sense that both input and output only depend on the job's metadata and
    data.

    This class is designed to define commands to be executed on the command
    line that constitute an operation.

    .. note::

            The command arguments should only depend on the job metadata to
            ensure deterministic operations.

    :param name: The name of this JobOperation instance. The name is arbitrary,
        but helps to concisely identify the operation in various contexts.
    :type name: str
    :param job: The job instance associated with this operation.
    :type job: :py:class:`signac.Job`.
    :param cmd: The command that constitutes the operation.
    :type cmd: str
    """

    def __init__(self, name, job, cmd):
        self.name = name
        self.job = job
        self.cmd = cmd

    def __str__(self):
        return self.name

    def get_id(self):
        "Return a name, which identifies this job-operation."
        return '{}-{}'.format(self.job, self.name)

    def __hash__(self):
        return int(sha1(self.get_id().encode('utf-8')).hexdigest(), 16)

    def __eq__(self, other):
        return self.get_id() == other.get_id()

    def set_status(self, value):
        "Store the operation's status."
        status_doc = self.job.document.get('status', dict())
        status_doc[self.get_id()] = int(value)
        self.job.document['status'] = status_doc

    def get_status(self):
        "Retrieve the operation's last known status."
        try:
            return self.job.document['status'][self.get_id()]
        except KeyError:
            return manage.JobStatus.unknown


class _FlowProjectClass(type):

    def __new__(metacls, name, bases, namespace, **kwargs):
        cls = type.__new__(metacls, name, bases, dict(namespace))
        cls._labels = {func for func in namespace.values() if _is_label_func(func)}
        return cls


class FlowProject(with_metaclass(_FlowProjectClass, signac.contrib.Project)):
    """A signac project class assisting in workflow management.

    :param config: A signac configuaration, defaults to
        the configuration loaded from the environment.
    :type config: A signac config object.
    """
    NAMES = {
        'next_operation': 'next_op',
    }

    @classmethod
    def _tr(cls, x):
        "Use name translation table for x."
        return cls.NAMES.get(x, x)

    ALIASES = dict(
        status='S',
        unknown='U',
        registered='R',
        queued='Q',
        active='A',
        inactive='I',
        requires_attention='!'
    )

    @classmethod
    def _alias(cls, x):
        "Use alias if specified."
        return abbreviate(x, cls.ALIASES.get(x, x))

    @classmethod
    def update_aliases(cls, aliases):
        "Update the ALIASES table for this class."
        cls.ALIASES.update(aliases)

    def _fn_bundle(self, bundle_id):
        return os.path.join(self.root_directory(), '.bundles', bundle_id)

    def _store_bundled(self, operations):
        """Store all job session ids part of one bundle.

        The job session ids are stored in a text file in the project's
        root directory. This is necessary to be able to identify each
        job's individual status from the bundle id."""
        if len(operations) == 1:
            return operations[0].get_id()
        else:
            h = '.'.join(op.get_id() for op in operations)
            bid = '{}-bundle-{}'.format(self, sha1(h.encode('utf-8')).hexdigest())
            fn_bundle = self._fn_bundle(bid)
            _mkdir_p(os.path.dirname(fn_bundle))
            with open(fn_bundle, 'w') as file:
                for operation in operations:
                    file.write(operation.get_id() + '\n')
            return bid

    def _expand_bundled_jobs(self, scheduler_jobs):
        "Expand jobs which were submitted as part of a bundle."
        for job in scheduler_jobs:
            if job.name().startswith('{}-bundle-'.format(self)):
                with open(self._fn_bundle(job.name())) as file:
                    for line in file:
                        yield manage.ClusterJob(line.strip(), job.status())
            else:
                yield job

    def scheduler_jobs(self, scheduler):
        """Fetch jobs from the scheduler.

        This function will fetch all scheduler jobs from the scheduler
        and also expand bundled jobs automatically.

        However, this function will not automatically filter scheduler
        jobs which are not associated with this project.

        :param scheduler: The scheduler instance.
        :type scheduler: :class:`~.flow.manage.Scheduler`
        :yields: All scheduler jobs fetched from the scheduler
            instance.
        """
        for sjob in self._expand_bundled_jobs(scheduler.jobs()):
            yield sjob

    @staticmethod
    def _map_scheduler_jobs(scheduler_jobs):
        "Map all scheduler jobs by job id and operation name."
        for sjob in scheduler_jobs:
            name = sjob.name()
            if name[32] == '-':
                yield name[:32], name[33:], sjob

    def map_scheduler_jobs(self, scheduler_jobs):
        """Map all scheduler jobs by job id.

        This function fetches all scheduled jobs from the scheduler
        and generates a nested dictionary, where the first key is
        the job id, the second key the operation name and the last
        value are the cooresponding scheduler jobs.

        For example, to print the status of all scheduler jobs, associated
        with a specific job operation, execute:

        .. code::

                sjobs = project.scheduler_jobs(scheduler)
                sjobs_map = project.map_scheduler_jobs(sjobs)
                for sjob in sjobs_map[job.get_id()][operation]:
                    print(sjob._id(), sjob.status())

        :param scheduler_jobs: An iterable of scheduler job instances.
        :returns: A nested dictionary (job_id, op_name, scheduler jobs)
        """
        sjobs_map = defaultdict(dict)
        for job_id, op, sjob in self._map_scheduler_jobs(scheduler_jobs):
            sjobs = sjobs_map[job_id].setdefault(op, list())
            sjobs.append(sjob)
        return sjobs_map

    def _update_status(self, job, scheduler_jobs):
        "Determine the scheduler status of job."
        manage.update_status(job, scheduler_jobs)

    def get_job_status(self, job):
        "Return the detailed status of a job."
        result = dict()
        result['job_id'] = str(job)
        status = job.document.get('status', dict())
        result['active'] = is_active(status)
        result['labels'] = sorted(set(self.classify(job)))
        result['operation'] = self.next_operation(job)
        highest_status = max(status.values()) if len(status) else 1
        result['submission_status'] = [manage.JobStatus(highest_status).name]
        return result

    def submit_user(self, env, _id, operations, walltime=None, force=False, **kwargs):
        """Implement this method to submit operations in combination with submit().

        The :py:func:`~.submit` method provides an interface for the submission of
        operations to the environment's scheduler. Operations will be optionally bundled
        into one submission and.

        The submit_user() method enables the user to create and submit a job submission
        script that controls the execution of all operations for this particular project.

        :param env: The environment to submit to.
        :type env: :class:`~.flow.manage.ComputeEnvironment`
        :param _id: A unique identifier, automatically calculated for this submission.
        :tpype _id: str
        :param operations: A list of operations that should be executed as part of this
            submission.
        :param walltime: The submission should be limited to the provided walltime.
        :type walltime: :py:class:`datetime.timedelta`
        :force: Warnings and other checks should be ignored if this argument is True.
        :type force: bool
        """
        raise NotImplementedError()

    def submit(self, env, job_ids=None, operation_name=None, walltime=None,
               num=None, force=False, bundle_size=1, cmd=None, requires=None,
               pool=None, **kwargs):
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
        # Backwards-compatilibity check...
        if isinstance(env, manage.Scheduler):
            raise ValueError(
                "The submit() API has changed with signac-flow version 0.4, "
                "please update your project. ")
        LEGACY = self._check_legacy_api()
        if LEGACY:
            logger.warning("Deprecated FlowProject API, switching to legacy mode.")
            warnings.warn(
                "Deprecated FlowProject API, switching to legacy mode.",
                PendingDeprecationWarning)

        if walltime is not None:
            walltime = datetime.timedelta(hours=walltime)

        if job_ids:
            jobs = (self.open_job(id=_id) for _id in job_ids)
        else:
            jobs = iter(self)

        def get_op(job):
            if LEGACY and operation_name is not None:
                return JobOperation(name=operation_name, job=job, cmd=None)
            if cmd is None:
                if LEGACY:
                    return JobOperation(name=self.next_operation(job), job=job, cmd=None)
                else:
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
            if LEGACY:
                return self.eligible(job=op.job, operation=op.name, **kwargs)
            else:
                return self.eligible_for_submission(op)

        # Get the first num eligible operations
        map_ = map if pool is None else pool.imap  # parallelization
        operations = islice((op for op in map_(get_op, jobs) if eligible(op)), num)

        # Bundle all eligible operations and submit the bundles
        for bundle in make_bundles(operations, bundle_size):
            _id = self._store_bundled(bundle)
            _submit = self._submit_legacy if LEGACY else self.submit_user
            status = _submit(
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

    @classmethod
    def add_submit_args(cls, parser):
        "Add arguments to parser for the :meth:`~.submit` method."
        parser.add_argument(
            'job_ids',
            type=str,
            nargs='*',
            help="The job id of the jobs to submit. "
            "Omit to automatically select all eligible jobs.")
        parser.add_argument(
            '-j', '--job-operation',
            dest='operation_name',
            type=str,
            help="Only submit jobs eligible for the specified operation.")
        parser.add_argument(
            '-w', '--walltime',
            type=float,
            default=DEFAULT_WALLTIME_HRS,
            help="The wallclock time in hours.")
        parser.add_argument(
            '--pretend',
            action='store_true',
            help="Do not really submit, but print the submittal script to screen.")
        parser.add_argument(
            '-n', '--num',
            type=int,
            help="Limit the number of operations to be submitted.")
        parser.add_argument(
            '--force',
            action='store_true',
            help="Ignore all warnings and checks, just submit.")
        parser.add_argument(
            '--bundle',
            type=int,
            nargs='?',
            const=0,
            default=1,
            dest='bundle_size',
            help="Specify how many operations to bundle into one submission. "
                 "When no specific size is give, all eligible operations are "
                 "bundled into one submission.")
        parser.add_argument(
            '-s', '--serial',
            action='store_true',
            help="Schedule the operations to be executed in serial.")
        parser.add_argument(
            '--after',
            type=str,
            help="Schedule this job to be executed after "
                 "completion of a cluster job with this id.")
        parser.add_argument(
            '--hold',
            action='store_true',
            help="All operations will be scheduled with a scheduler hold.")
        parser.add_argument(
            '--cmd',
            type=str,
            help="Directly specify the command that executes the desired operation.")
        parser.add_argument(
            '--requires',
            type=str,
            nargs='*',
            help="Manually specify all labels, that are required for a job to be "
                 "considered eligible for submission. This is especially useful "
                 "in combination with '--cmd'.")

    def write_human_readable_statepoint(self, script, job):
        "Write statepoint of job in human-readable format to script."
        script.write('# Statepoint:\n#\n')
        sp_dump = json.dumps(job.statepoint(), indent=2).replace(
            '{', '{{').replace('}', '}}')
        for line in sp_dump.splitlines():
            script.write('# ' + line + '\n')
        script.write('\n')

    def _print_overview(self, stati, max_lines=None, file=sys.stdout):
        "Print the project's status overview."
        progress = defaultdict(int)
        for status in stati:
            for label in status['labels']:
                progress[label] += 1
        progress_sorted = islice(sorted(
            progress.items(), key=lambda x: (x[1], x[0]), reverse=True), max_lines)
        table_header = ['label', 'progress']
        rows = ([label, '{} {:0.2f}%'.format(
            draw_progressbar(num, len(stati)), 100 * num / len(stati))]
            for label, num in progress_sorted)
        print("{} {}".format(self._tr("Total # of jobs:"), len(stati)), file=file)
        print(util.tabulate.tabulate(rows, headers=table_header), file=file)
        if max_lines is not None:
            lines_skipped = len(progress) - max_lines
            if lines_skipped:
                print("{} {}".format(self._tr("Lines omitted:"), lines_skipped), file=file)

    def format_row(self, status, statepoint=None, max_width=None):
        "Format each row in the detailed status output."
        row = [
            status['job_id'],
            ', '.join((self._alias(s) for s in status['submission_status'])),
            status['operation'],
            ', '.join(status.get('labels', [])),
        ]
        if statepoint:
            sps = self.open_job(id=status['job_id']).statepoint()

            def get(k, m):
                if m is None:
                    return
                t = k.split('.')
                if len(t) > 1:
                    return get('.'.join(t[1:]), m.get(t[0]))
                else:
                    return m.get(k)

            for i, k in enumerate(statepoint):
                v = self._alias(get(k, sps))
                row.insert(i + 3, None if v is None else shorten(str(v), max_width))
        if status['operation'] and not status['active']:
            row[1] += ' ' + self._alias('requires_attention')
        return row

    def _print_detailed(self, stati, parameters=None,
                        skip_active=False, param_max_width=None,
                        file=sys.stdout):
        "Print the project's detailed status."
        table_header = [self._tr(self._alias(s))
                        for s in ('job_id', 'status', 'next_operation', 'labels')]
        if parameters:
            for i, value in enumerate(parameters):
                table_header.insert(i + 3, shorten(self._alias(str(value)), param_max_width))
        rows = (self.format_row(status, parameters, param_max_width)
                for status in stati if not (skip_active and status['active']))
        print(util.tabulate.tabulate(rows, headers=table_header), file=file)
        if abbreviate.table:
            print(file=file)
            print(self._tr("Abbreviations used:"), file=file)
            for a in sorted(abbreviate.table):
                print('{}: {}'.format(a, abbreviate.table[a]), file=file)

    def export_job_stati(self, collection, stati):
        "Export the job stati to a database collection."
        for status in stati:
            job = self.open_job(id=status['job_id'])
            status['statepoint'] = job.statepoint()
            collection.update_one({'_id': status['job_id']},
                                  {'$set': status}, upsert=True)

    def update_stati(self, scheduler, jobs=None, file=sys.stderr, pool=None):
        """Update the status of all jobs with the given scheduler.

        :param scheduler: The scheduler instance used to feth the job stati.
        :type scheduler: :class:`~.manage.Scheduler`
        :param jobs: A sequence of :class:`~.signac.contrib.Job` instances.
        :param file: The file to write output to, defaults to `sys.stderr`."""
        if jobs is None:
            jobs = self.find_jobs()
        print(self._tr("Query scheduler..."), file=file)
        sjobs_map = defaultdict(list)
        for sjob in self.scheduler_jobs(scheduler):
            sjobs_map[sjob.name()].append(sjob)
        print(self._tr("Determine job stati..."), file=file)
        if pool is None:
            for job in tqdm(jobs, file=file):
                self._update_status(job, sjobs_map)
        else:
            jobs_ = ((job, sjobs_map) for job in jobs)
            pool.map(_update_status, tqdm(jobs_, total=len(jobs), file=file))

    def print_status(self, scheduler=None, job_filter=None,
                     overview=True, overview_max_lines=None,
                     detailed=False, parameters=None, skip_active=False,
                     param_max_width=None,
                     file=sys.stdout, err=sys.stderr,
                     pool=None):
        """Print the status of the project.

        :param scheduler: The scheduler instance used to fetch the job stati.
        :type scheduler: :class:`~.manage.Scheduler`
        :param job_filter: A JSON encoded filter,
            that all jobs to be submitted need to match.
        :param overview: Aggregate an overview of the project' status.
        :type overview: bool
        :param overview_max_lines: Limit the number of overview lines.
        :type overview_max_lines: int
        :param detailed: Print a detailed status of each job.
        :type detailed: bool
        :param parameters: Print the value of the specified parameters.
        :type parameters: list of str
        :param skip_active: Only print jobs that are currently inactive.
        :type skip_active: bool
        :param param_max_width: Limit the number of characters of parameter
            columns, see also: :py:meth:`~.update_aliases`.
        :param file: Redirect all output to this file,
            defaults to sys.stdout
        :param err: Redirect all error output to this file,
            defaults to sys.stderr
        :param pool: A multiprocessing or threading pool. Providing a pool
            parallelizes this method."""
        if job_filter is not None and isinstance(job_filter, str):
            job_filter = json.loads(job_filter)
        jobs = list(self.find_jobs(job_filter))
        if scheduler is not None:
            self.update_stati(scheduler, jobs, file=err, pool=pool)
        print(self._tr("Generate output..."), file=err)
        if pool is None:
            stati = [self.get_job_status(job) for job in jobs]
        else:
            stati = pool.map(self.get_job_status, jobs)
        title = "{} '{}':".format(self._tr("Status project"), self)
        print('\n' + title, file=file)
        if overview:
            self._print_overview(stati, max_lines=overview_max_lines, file=file)
        if detailed:
            print(file=file)
            print(self._tr("Detailed view:"), file=file)
            self._print_detailed(stati, parameters, skip_active,
                                 param_max_width, file)

    @classmethod
    def add_print_status_args(cls, parser):
        "Add arguments to parser for the :meth:`~.print_status` method."
        parser.add_argument(
            '-f', '--filter',
            dest='job_filter',
            type=str,
            help="Filter jobs.")
        parser.add_argument(
            '--no-overview',
            action='store_false',
            dest='overview',
            help="Do not print an overview.")
        parser.add_argument(
            '-m', '--overview-max-lines',
            type=int,
            help="Limit the number of lines in the overview.")
        parser.add_argument(
            '-d', '--detailed',
            action='store_true',
            help="Display a detailed view of the job stati.")
        parser.add_argument(
            '-p', '--parameters',
            type=str,
            nargs='*',
            help="Display select parameters of the job's "
                 "statepoint with the detailed view.")
        parser.add_argument(
            '--param-max-width',
            type=int,
            help="Limit the width of each parameter row.")
        parser.add_argument(
            '--skip-active',
            action='store_true',
            help="Display only jobs, which are currently not active.")

    def labels(self, job):
        """Auto-generate labels from label-functions.

        This generator function will automatically yield labels,
        from project methods decorated with the ``@label`` decorator.

        For example, we can define a function like this:

        .. code-block:: python

            class MyProject(FlowProject):

                @label()
                def is_foo(self, job):
                    return job.document.get('foo', False)

        The ``labels()`` generator method will now yield a ``is_foo``
        label whenever the job document has a field ``foo`` which
        evaluates to True.

        By default, the label name is equal to the function's name,
        but you can specify a custom label as the first argument to the
        label decorator, e.g.: ``@label('foo_label')``.

        .. tip::

            In this particular case it may make sense to define the
            ``is_foo()`` method as a *staticmethod*, since it does not
            actually depend on the project instance. We can do this by
            using the ``@staticlabel()`` decorator, equivalently the
            ``@classlabel()`` for *class methods*.
        """
        for label in self._labels:
            if hasattr(label, '__func__'):
                label = getattr(self, label.__func__.__name__)
                if label(job):
                    yield getattr(label, '_label_name', label.__name__)
            elif label(self, job):
                yield getattr(label, '_label_name', label.__name__)

    def classify(self, job):
        """Generator function which yields labels for job.

        :param job: The signac job handle.
        :type job: :class:`~signac.contrib.job.Job`
        :yields: The labels to classify job.
        :yield type: str"""
        yield
        return

    def next_operation(self, job):
        """Determine the next operation for this job.

        :param job: The signac job handle.
        :type job: :class:`~signac.contrib.job.Job`
        :returns: A JobOpereation instance to execute next.
        :rtype: :py:class:`~.JobOperation`
        """
        return

    def eligible(self, job_operation, **kwargs):
        """Determine if job is eligible for operation.

        .. warning::

            This function is deprecated, please use
            :py:meth:`~.eligible_for_submission` instead.
        """
        return None

    def eligible_for_submission(self, job_operation):
        "Determine if a job-operation is eligible for submission."
        if job_operation is None:
            return False
        if job_operation.get_status() >= manage.JobStatus.submitted:
            return False
        return True

    def _check_legacy_api(self):
        return hasattr(self, 'write_user')

    def _submit_legacy(self, env, _id, operations, walltime, force,
                       serial=False, ppn=None, pretend=False, after=None, hold=False,
                       **kwargs):
        if 'np' in kwargs:
            kwargs['nc'] = kwargs.pop('np')

        mpi_cmd = getattr(env, 'mpi_cmd', None)

        if mpi_cmd is None:

            def mpi_cmd(cmd, np=1):
                if np > 1:
                    return 'mpirun -np {} {}'.format(np, cmd)
                else:
                    return cmd

        if ppn is None:
            ppn = getattr(env, 'cores_per_node', 1)
            if ppn is None:
                ppn = 1

        class JobScriptLegacy(io.StringIO):
            "Simple StringIO wrapper to implement cmd wrapping logic."
            parallel = False

            def writeline(self, line, eol='\n'):
                "Write one line to the job script."
                self.write(line + eol)

            def write_cmd(self, cmd, parallel=False, np=1, mpi_cmd=None, **kwargs):
                "Write a command to the jobscript."
                if np > 1:
                    if mpi_cmd is None:
                        raise RuntimeError("Requires mpi_cmd wrapper.")
                    cmd = mpi_cmd(cmd, np=np)
                if parallel:
                    self.parallel = True
                    cmd += ' &'
                self.writeline(cmd)
                return np

        script = JobScriptLegacy()

        nps = list()
        for op in operations:
            nps.append(self.write_user(
                script=script,
                job=op.job,
                operation=op.name,
                parallel=not serial,
                mpi_cmd=mpi_cmd,
                **kwargs))
        if script.parallel:
            script.writeline('wait')
        script.seek(0)

        np = max(nps) if serial else sum(nps)
        nn = ceil(np / ppn)

        sscript = env.script(_id=_id, nn=nn, ppn=ppn, walltime=walltime, **kwargs)
        sscript.write(script.read())
        sscript.seek(0)
        return env.submit(sscript, pretend=pretend, hold=hold, after=after)
