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
import argparse
import datetime
import json
import errno
import subprocess
from collections import defaultdict
from itertools import islice
from hashlib import sha1
from math import ceil
from multiprocessing import Pool
from multiprocessing import TimeoutError
from subprocess import TimeoutExpired

import signac
from signac.common import six
from signac.common.six import with_metaclass

from .environment import get_environment
from .environment import NodesEnvironment
from . import manage
from . import util
from .errors import SubmitError
from .errors import NoSchedulerError
from .util.tqdm import tqdm

logger = logging.getLogger(__name__)


def _mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as error:
        if not (error.errno == errno.EEXIST and os.path.isdir(path)):
            raise


def _execute(cmd, timeout=None):
    if six.PY2:
        subprocess.call(cmd, timeout=timeout)
    else:
        subprocess.run(cmd, timeout=timeout)


def _show_cmd(cmd, timeout=None):
    print(' '.join(cmd))


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
    :type cmd: str
    """

    def __init__(self, name, job, cmd, np=None, mpi=False):
        self.name = name
        self.job = job
        self.cmd = cmd
        self.np = np
        self.mpi = mpi

    def __str__(self):
        return self.name

    def __repr__(self):
        return "{type}(name='{name}', job='{job}', cmd={cmd}, np={np})".format(
            type=type(self).__name__,
            name=self.name,
            job=str(self.job),
            cmd=repr(self.cmd),
            np=self.np)

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


class FlowCondition:

    def __init__(self, callback):
        self._callback = callback

    def __call__(self, job):
        if self._callback is None:
            return True
        return self._callback(job)

    def __hash__(self):
        return hash(self._callback)

    def __eq__(self, other):
        return self._callback == other._callback


class FlowOperation:

    def __init__(self, cmd, prereqs=None, postconds=None, np=None, mpi=False):
        if prereqs is None:
            prereqs = []
        if postconds is None:
            postconds = []
        if np is None:
            np = 1
        self._cmd = cmd
        self._np = np
        self.mpi = False

        self._prerequistes = [FlowCondition(cond) for cond in prereqs]
        self._postconditions = [FlowCondition(cond) for cond in postconds]

    def __str__(self):
        return "{type}(cmd='{cmd}')".format(type=type(self).__name__, cmd=self._cmd)

    def eligible(self, job):
        "Eligible, when all preconditions are true and at least one post condition is false."
        pre = all([cond(job) for cond in self._prerequistes])
        if len(self._postconditions):
            post = any([not cond(job) for cond in self._postconditions])
        else:
            post = True
        return pre and post

    def complete(self, job):
        return not self.eligible(job)

    def __call__(self, job=None):
        if callable(self._cmd):
            return self._cmd(job)
        else:
            return self._cmd.format(job=job)

    def np(self, job):
        if callable(self._np):
            return self._np(job)
        else:
            return self._np


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

    def __init__(self, config=None):
        signac.contrib.Project.__init__(self, config)
        self._operations = dict()

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

    def run(self, names=None, pretend=False, np=None, timeout=None, progress=False):
        names = set() if names is None else set(names)

        def select(op):
            if op is None:
                return False
            if names:
                return op.name in names
            else:
                return True

        def _cmd(op):
            return op.cmd.format(job=op.job).split()

        ops = (self.next_operation(job) for job in self)
        cmds = [_cmd(op) for op in ops if select(op)]

        _run = _show_cmd if pretend else _execute

        if np == 1:
            for cmd in tqdm(cmds) if progress else cmds:
                _run(cmd, timeout=timeout)
        elif six.PY2:
            # Due to Python 2.7 issue #8296 (http://bugs.python.org/issue8296) we
            # always need to provide a timeout to avoid issues with "hanging"
            # processing pools.
            timeout = sys.maxint if timeout is None else timeout
            pool = Pool(np)
            result = pool.imap_unordered(_run, cmds)
            for _ in tqdm(cmds) if progress else cmds:
                result.next(timeout)
        else:
            with Pool(np) as pool:
                result = pool.imap_unordered(_run, cmds)
                for _ in tqdm(cmds) if progress else cmds:
                    result.next(timeout)

    def write_script_header(self, script, **kwargs):
        # Add some whitespace
        script.writeline()
        # Don't use uninitialized environment variables.
        script.writeline('set -u')
        # Exit on errors.
        script.writeline('set -e')
        # Switch into the project root directory
        script.writeline('cd {}'.format(self.root_directory()))
        script.writeline()

    def write_script_operations(self, script, operations, background=False):
        "Iterate over all job-operations and write the command to the script."
        for op in operations:
            self.write_human_readable_statepoint(script, op.job)
            if op.mpi:
                script.write_cmd(op.cmd.format(job=op.job), np=op.np, bg=background)
            else:
                script.write_cmd(op.cmd.format(job=op.job), bg=background)
            script.writeline()

    def write_script_footer(self, script, **kwargs):
        # Wait until all processes have finished
        script.writeline('wait')

    def write_script(self, script, operations, background=False):
        self.write_script_header(script)
        self.write_script_operations(script, operations, background=background)
        self.write_script_footer(script)

    def submit_user(self, env, _id, operations, nn=None, ppn=None, serial=False,
                    flags=None, force=False, **kwargs):
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
        if issubclass(env, NodesEnvironment):
            if nn is None:
                if serial:
                    np_total = max(op.np for op in operations)
                else:
                    np_total = sum(op.np for op in operations)
                try:
                    nn = env.calc_num_nodes(np_total, ppn, force)
                except SubmitError as e:
                    if not (flags or force):
                        raise e

        script = env.script(_id=_id, nn=nn, ppn=ppn, **kwargs)
        self.write_script(script, operations, background=not serial)
        return env.submit(script, nn=nn, ppn=ppn, flags=flags, **kwargs)

    def gather_operations(self, job_id=None, operation_name=None, num=None, bundle_size=1,
                          cmd=None, requires=None, pool=None, serial=False, force=False, **kwargs):
        if job_id:
            jobs = (self.open_job(id=_id) for _id in job_id)
        else:
            jobs = iter(self)

        def get_op(job):
            if cmd is None:
                return self.next_operation(job)
            else:
                return JobOperation(name='user-cmd', cmd=cmd.format(job=job), job=job)

        def eligible(op):
            if op is None:
                return False
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
        map_ = map if pool is None else pool.imap  # parallelization
        return islice((op for op in map_(get_op, jobs) if eligible(op)), num)

    def submit(self, env, bundle_size=1, serial=False, force=False,
               nn=None, ppn=None, walltime=None, **kwargs):
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
        if self._check_legacy_api():
            raise DeprecationWarning(
                "You are using a deprecated FlowProject API, please update your project.")

        if walltime is not None:
            walltime = datetime.timedelta(hours=walltime)

        operations = self.gather_operations(**kwargs)
        for bundle in make_bundles(operations, bundle_size):
            _id = self._store_bundled(bundle)
            status = self.submit_user(env=env, _id=_id, operations=bundle, nn=nn, ppn=ppn,
                                      serial=serial, force=force, walltime=walltime, **kwargs)
            if status is not None:
                for op in bundle:
                    op.set_status(status)

    @classmethod
    def add_submit_args(cls, parser):
        "Add arguments to parser for the :meth:`~.submit` method."

        parser.add_argument(
            'flags',
            type=str,
            nargs='*',
            help="Flags to be forwarded to the scheduler.")
        parser.add_argument(
            '--pretend',
            action='store_true',
            help="Do not really submit, but print the submittal script to screen.")
        parser.add_argument(
            '--force',
            action='store_true',
            help="Ignore all warnings and checks, just submit.")
        cls.add_script_args(parser)

    @classmethod
    def add_script_args(cls, parser):
        "Add arguments to parser for the :meth:`~.submit` method."
        parser.add_argument(
            '-j', '--job-id',
            type=str,
            nargs='+',
            help="The job id of the jobs to submit. "
            "Omit to automatically select all eligible jobs.")
        selection_group = parser.add_argument_group('job operation selection')
        selection_group.add_argument(
            '-o', '--operation',
            dest='operation_name',
            type=str,
            help="Only submit jobs eligible for the specified operation.")
        selection_group.add_argument(
            '-n', '--num',
            type=int,
            help="Limit the number of operations to be submitted.")

        bundling_group = parser.add_argument_group('bundling')
        bundling_group.add_argument(
            '--bundle',
            type=int,
            nargs='?',
            const=0,
            default=1,
            dest='bundle_size',
            help="Specify how many operations to bundle into one submission. "
                 "When no specific size is give, all eligible operations are "
                 "bundled into one submission.")
        bundling_group.add_argument(
            '-s', '--serial',
            action='store_true',
            help="Schedule the operations to be executed in serial.")

        manual_cmd_group = parser.add_argument_group("manual cmd")
        manual_cmd_group.add_argument(
            '--cmd',
            type=str,
            help="Directly specify the command that executes the desired operation.")
        manual_cmd_group.add_argument(
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

    def _print_overview(self, stati, max_lines=None, file=sys.stdout):
        "Print the project's status overview."
        progress = defaultdict(int)
        for status in stati:
            for label in status['labels']:
                progress[label] += 1
        print("{} {}\n".format(self._tr("Total # of jobs:"), len(stati)), file=file)
        progress_sorted = list(islice(sorted(
            progress.items(), key=lambda x: (x[1], x[0]), reverse=True), max_lines))
        table_header = ['label', 'progress']
        if progress_sorted:
            rows = ([label, '{} {:0.2f}%'.format(
                draw_progressbar(num, len(stati)), 100 * num / len(stati))]
                for label, num in progress_sorted)
            print(util.tabulate.tabulate(rows, headers=table_header), file=file)
            if max_lines is not None:
                lines_skipped = len(progress) - max_lines
                if lines_skipped:
                    print("{} {}".format(self._tr("Lines omitted:"), lines_skipped), file=file)
        else:
            print(util.tabulate.tabulate([], headers=table_header), file=file)
            print("[no labels]", file=file)

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

    def update_stati(self, scheduler, jobs=None, file=sys.stderr, pool=None, ignore_errors=False):
        """Update the status of all jobs with the given scheduler.

        :param scheduler: The scheduler instance used to feth the job stati.
        :type scheduler: :class:`~.manage.Scheduler`
        :param jobs: A sequence of :class:`~.signac.contrib.Job` instances.
        :param file: The file to write output to, defaults to `sys.stderr`."""
        if jobs is None:
            jobs = self.find_jobs()
        print(self._tr("Query scheduler..."), file=file)
        sjobs_map = defaultdict(list)
        try:
            for sjob in self.scheduler_jobs(scheduler):
                sjobs_map[sjob.name()].append(sjob)
        except RuntimeError as e:
            if ignore_errors:
                logger.warning("WARNING: Error while queyring scheduler: '{}'.".format(e))
            else:
                raise RuntimeError("Error while querying scheduler: '{}'.".format(e))
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
                     pool=None, ignore_errors=False):
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
            self.update_stati(scheduler, jobs, file=err, pool=pool, ignore_errors=ignore_errors)
        print(self._tr("Generate output..."), file=err)
        if pool is None:
            stati = [self.get_job_status(job) for job in jobs]
        else:
            stati = list(pool.map(self.get_job_status, jobs))
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
        parser.add_argument(
            '--ignore-errors',
            action='store_true',
            help="Ignore errors that might occur when querying the scheduler.")

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

    def add_operation(self, name, cmd, prereqs=None, postconds=None, np=None, **kwargs):
        """
        Add an operation to the workflow.

        :param name: Name of the operation.
        :type name: str
        :param cmd: script that will execute the operation from the command line.
                    Usually contains {job} format field to specify which job the operation
                    will run for.
        :type cmd: str
        :param prereqs: list of unary functions that return a bool value. All prequisites
            must be true for a job to be considered 'eligible' for execution.
        :type prereqs: list
        :param postconds: list of unary functions that return a bool value. All post conditions
            must be true for a job to be considered 'completed' for execution.
        :type postconds: list

        A note on job eligibilty: As stated above all prereqs must evaluate to true and at least
        one post condition must evaluate to false. When all prereqs and post conditions are true
        then the job operation is considered to be complete.

        Eligibility in this contexts refers only to the workflow pipline and not to
        other contributing factors like the whether the job-operation is currently
        running or queued.
        """
        self._operations[name] = FlowOperation(
            cmd=cmd, prereqs=prereqs, postconds=postconds, np=np, **kwargs)

    def classify(self, job):
        """Generator function which yields labels for job.

        :param job: The signac job handle.
        :type job: :class:`~signac.contrib.job.Job`
        :yields: The labels to classify job.
        :yield type: str
        """
        for label in self.labels(job):
            yield label

    def completed_operations(self, job):
        """Determine which operations have been completed for job.

        :param job: The signac job handle.
        :type job: :class:`~signac.contrib.job.Job`
        :returns: The name of the operations that are complete.
        :rtype: str"""
        for name, op in self._operations.items():
            if op.complete(job):
                yield name

    def next_operations(self, job):
        """Determine the next operations for job.

        You can, but don't have to use this function to simplify
        the submission process. The default method returns the next
        operation for the job as defined by the 'add_operation' method.

        :param job: The signac job handle.
        :type job: :class:`~signac.contrib.job.Job`
        :returns: The name of the operation to execute next.
        :rtype: str"""
        for name, op in self.operations.items():
            if op.eligible(job):
                yield JobOperation(name=name, job=job, cmd=op(job), np=op.np(job), mpi=op.mpi)

    def next_operation(self, job):
        """Determine the next operation for this job.

        :param job: The signac job handle.
        :type job: :class:`~signac.contrib.job.Job`
        :returns: A JobOpereation instance to execute next or `None` if no operation is eligible.
        :rtype: :py:class:`~.JobOperation` or `NoneType`
        """
        for op in self.next_operations(job):
            return op

    @property
    def operations(self):
        """
        The dictionary of operations that have been added to the workflow.
        :return: a dictionary with the key values are the names of the operations and
                 the values are instances of :py:class:`~.FlowOperation`
        :rtype: dict
        """
        return self._operations

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

        self.write_header(script, walltime, **kwargs)

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

    def main(self, parser=None, pool=None):

        def _status(env, args):
            args = vars(args)
            del args['func']
            try:
                self.print_status(env.get_scheduler(), pool=pool, **args)
            except NoSchedulerError:
                self.print_status(None, pool=pool, **args)
            return 0

        def _next(env, args):
            for job in self:
                next_op = self.next_operation(job)
                if next_op is not None and next_op.name == args.name:
                    print(job)

        def _run(env, args):
            try:
                self.run(names=args.name, pretend=args.pretend, np=args.np,
                         timeout=args.timeout, progress=args.progress)
            except (TimeoutError, TimeoutExpired):
                print(
                    "Error: Failed to complete due to timeout ({}s)!".format(args.timeout),
                    file=sys.stderr)
                return 1
            return 0

        def _script(env, args):
            kwargs = vars(args)
            del kwargs['func']
            env = get_environment(test=True)
            ops = self.gather_operations(**kwargs)
            for bundle in make_bundles(ops, args.bundle_size):
                script = env.script()
                script.writeline("# --------- BEGIN SCRIPT ----------")
                self.write_script(script, bundle, background=not args.serial)
                script.writeline("# ---------- END SCRIPT -----------")
                script.seek(0)
                print(script.read())

        def _submit(env, args):
            kwargs = vars(args)
            del kwargs['func']
            debug = kwargs.pop('debug')
            test = kwargs.pop('test')
            if test:
                env = get_environment(test=True)
            try:
                self.submit(env, **kwargs)
            except NoSchedulerError as e:
                print(
                    "Error:", e, "Consider using '--test', for testing purposes.", file=sys.stderr)
                if debug:
                    raise
                return 1
            except SubmitError as e:
                print("Submission error:", e, file=sys.stderr)
                if debug:
                    raise
                return 1
            else:
                return 0

        env = get_environment()

        if parser is None:
            parser = argparse.ArgumentParser()

        subparsers = parser.add_subparsers()

        parser_status = subparsers.add_parser('status')
        self.add_print_status_args(parser_status)
        parser_status.set_defaults(func=_status)

        parser_next = subparsers.add_parser(
            'next',
            description="Determine jobs that are eligible for a specific operation.")
        parser_next.add_argument(
            'name',
            type=str,
            help="The name of the operation.")
        parser_next.set_defaults(func=_next)

        parser_run = subparsers.add_parser('run')
        parser_run.add_argument(
            'name',
            type=str,
            nargs='*',
            help="If provided, only run operations where the identifier "
                 "matches the provided set of names.")
        parser_run.add_argument(
            '-p', '--pretend',
            action='store_true',
            help="Do not actually execute commands, just show them.")
        parser_run.add_argument(
            '--np',
            type=int,
            help="Specify the number of cores to parallelize to. The "
                 "default value of 0 means as many cores as are available.")
        parser_run.add_argument(
            '-t', '--timeout',
            type=int,
            help="A timeout in seconds after which the parallel execution "
                 "of operations is canceled.")
        parser_run.add_argument(
            '--progress',
            action='store_true',
            help="Display a progress bar during execution.")
        parser_run.set_defaults(func=_run)

        parser_script = subparsers.add_parser('script')
        self.add_script_args(parser_script)
        parser_script.set_defaults(func=_script)

        parser_submit = subparsers.add_parser('submit')
        self.add_submit_args(parser_submit)
        parser_submit.add_argument(
            '-d', '--debug',
            action="store_true",
            help="Print debugging information.")
        parser_submit.add_argument(
            '-t', '--test',
            action='store_true')
        env_group = parser_submit.add_argument_group('{} options'.format(env.__name__))
        env.add_args(env_group)
        parser_submit.set_defaults(func=_submit)

        args = parser.parse_args()
        if not hasattr(args, 'func'):
            parser.print_usage()
            sys.exit(2)

        sys.exit(args.func(env, args))
