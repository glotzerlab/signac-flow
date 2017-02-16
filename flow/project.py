# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import sys
import os
import logging
import warnings
import datetime
import json
import errno
from collections import defaultdict
from itertools import islice
from hashlib import sha1

import signac
from signac.common.six import with_metaclass

from . import manage
from . import util
from .fakescheduler import FakeScheduler
from .moab import MoabScheduler
from .slurm import SlurmScheduler
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
    for gid, s in status.items():
        if s > manage.JobStatus.inactive:
            return True
    return False


def draw_progressbar(value, total, width=40):
    n = int(value / total * width)
    return '|' + ''.join(['#'] * n) + ''.join(['-'] * (width - n)) + '|'


def abbreviate(x, a):
    if x == a:
        return x
    else:
        abbreviate.table[a] = x
        return a
abbreviate.table = dict()  # noqa


def shorten(x, max_length=None):
    if max_length is None:
        return x
    else:
        return abbreviate(x, x[:max_length])


def _update_status(args):
    return manage.update_status(* args)


class label(object):

    def __init__(self, name=None):
        self.name = name

    def __call__(self, func):
        func._label = True
        if self.name is not None:
            func._label_name = self.name
        return func


class staticlabel(label):

    def __call__(self, func):
        return staticmethod(super(staticlabel, self).__call__(func))


class classlabel(label):

    def __call__(self, func):
        return classmethod(super(classlabel, self).__call__(func))


def _is_label_func(func):
    return getattr(getattr(func, '__func__', func), '_label', False)


def make_bundles(operations, size=None):
    n = None if size == 0 else size
    while True:
        b = list(islice(operations, n))
        if b:
            yield b
        else:
            break


class JobOperation:

    def __init__(self, name, job, cmd):
        self.name = name
        self.job = job
        self.cmd = cmd

    def __str__(self):
        return self.name

    def get_id(self):
        return '{}-{}'.format(self.job, self.name)

    def __hash__(self):
        return int(sha1(self.get_id().encode('utf-8')).hexdigest(), 16)

    def __eq__(self, other):
        return self.get_id() == other.get_id()

    def set_status(self, value):
        "Update the job's status dictionary."
        status_doc = self.job.document.get('status', dict())
        status_doc[self.get_id()] = int(value)
        self.job.document['status'] = status_doc

    def get_status(self):
        try:
            return self.job.document['status'][self.get_id()]
        except KeyError:
            return manage.JobStatus.unknown


class _FlowProjectClass(type):

    def __new__(metacls, name, bases, namespace, **kwrgs):
        cls = type.__new__(metacls, name, bases, dict(namespace))
        cls._labels = {func for func in namespace.values() if _is_label_func(func)}
        return cls


class FlowProject(with_metaclass(_FlowProjectClass, signac.contrib.Project)):
    """A signac project class assisting in job scheduling.

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
        "Return the detailed status of jobs."
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
        raise NotImplementedError()

    def submit(self, env, job_ids=None, operation_name=None, walltime=None,
               num=None, force=False, bundle_size=1, cmd=None, requires=None, **kwargs):
        """Submit jobs to the scheduler.

        :param env: The env instance.
        :type env: :class:`~.flow.manage.ComputeEnvironment`
        :param job_ids: A list of job_id's,
            defaults to all jobs found in the workspace.
        :param operation: A specific operation,
            defaults to the result of :meth:`~.next_operation`.
        :param job_filter: A JSON encoded filter that all jobs
            to be submitted need to match.
        :param walltime: The maximum wallclock time in hours.
        :type walltime: float
        :param bundle: Bundle up to 'bundle' number of jobs during submission.
        :type bundle: int
        :param serial: Schedule jobs in serial or execute bundled
            jobs in serial.
        :type serial: bool
        :param after: Execute all jobs after the completion of the job operation
            with this job session id. Implementation is scheduler dependent.
        :type after: str
        :param num: Do not submit more than 'num' jobs to the scheduler.
        :type num: int
        :param pretend: Do not actually submit, but instruct the scheduler
            to pretend scheduling.
        :type pretend: bool
        :param force: Ignore all eligibility checks, just submit.
        :type force: bool
        :param kwargs: Other keyword arguments which are forwareded."""
        # Backwards-compatilibity check...
        if any((isinstance(env, s) for s in (FakeScheduler, MoabScheduler, SlurmScheduler))):
            # Entering legacy mode!
            from .project_legacy import submit as submit_legacy
            logger.warning("You are using a deprecated FlowProject API!")
            warnings.warn("You are using a deprecated FlowProject API!", DeprecationWarning)
            warnings.warn("You are using a deprecated FlowProject API!")  # higher visibility
            submit_legacy(self, scheduler=env, job_ids=job_ids, operation=operation_name,
                          walltime=walltime, num=num, force=force, bundle=bundle_size,
                          **kwargs)
            return

        if walltime is not None:
            walltime = datetime.timedelta(hours=walltime)

        if job_ids:
            jobs = [self.open_job(id=_id) for _id in job_ids]
        else:
            jobs = iter(self)

        if cmd is None:
            def op(job):
                op = self.next_operation(job)
                if isinstance(op, str):
                    warnings.warn("Returning job-operations as str is deprecated.",
                                  DeprecationWarning)
                    op = JobOperation(op, job, 'python scripts/run.py {} {}'.format(op, job))
                if operation_name is None or op.name == operation_name:
                    return op
        else:
            def op(job):
                if requires is not None:
                    labels = list(self.labels(job))
                    if not all([req in labels for req in requires]):
                        return
                job.ws = job.workspace()  # Extending the job namespace
                return JobOperation(name='user-cmd', cmd=cmd.format(job=job), job=job)

        operations = filter(None, map(op, jobs))
        operations = (op for op in operations if op.get_status() < manage.JobStatus.submitted)
        operations = islice(operations, num)

        for bundle in make_bundles(operations, bundle_size):
            status = self.submit_user(
                env=env,
                _id=self._store_bundled(bundle),
                operations=bundle,
                walltime=walltime,
                force=force,
                **kwargs)
            if status:
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
            help="Do not really submit, but print the submittal script.")
        parser.add_argument(
            '-n', '--num',
            type=int,
            help="Limit the number of jobs submitted at once.")
        parser.add_argument(
            '--force',
            action='store_true',
            help="Do not check job status or classification, just submit.")
        parser.add_argument(
            '--bundle',
            type=int,
            nargs='?',
            const=0,
            default=1,
            dest='bundle_size',
            help="Specify how many jobs to bundle into one submission. "
                 "Omit a specific value to bundle all eligible jobs.")
        parser.add_argument(
            '-s', '--serial',
            action='store_true',
            help="Schedule the jobs to be executed serially.")
        parser.add_argument(
            '--after',
            type=str,
            help="Schedule this job to be executed after "
                 "completion of job with this id.")
        parser.add_argument(
            '--hold',
            action='store_true',
            help="Submit job with user hold applied.")
        parser.add_argument(
            '--cmd',
            type=str,
            help="Submit this command to the scheduler for jobs"
                 "with lables specified with --requires argument.")
        parser.add_argument(
            '--requires',
            type=str,
            nargs='*',
            help="the labels required for the job to be considered eligible used"
                 "with the --cmd option otherwise it is ignored.")

    def write_human_readable_statepoint(self, script, job):
        "Write statepoint of job in human-readable format to script."
        script.write('# Statepoint:\n#\n')
        sp_dump = json.dumps(job.statepoint(), indent=2).replace(
            '{', '{{').replace('}', '}}')
        for line in sp_dump.splitlines():
            script.write('# ' + line + '\n')
        script.write('\n')

    def print_overview(self, stati, max_lines=None, file=sys.stdout):
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

    def print_detailed(self, stati, parameters=None,
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
        :param detailed: Print a detailed status of each job.
        :type detailed: bool
        :param parameters: Print the value of the specified parameters.
        :type parameters: list of str
        :param skip_active: Only print jobs that are currently inactive.
        :type skip_active: bool
        :param file: Print all output to this file,
            defaults to sys.stdout
        :param err: Print all error output to this file,
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
            self.print_overview(stati, max_lines=overview_max_lines, file=file)
        if detailed:
            print(file=file)
            print(self._tr("Detailed view:"), file=file)
            self.print_detailed(stati, parameters, skip_active,
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

    def next_operation(self, job):
        """Determine the next operation for job.

        You can, but don't have to use this function to simplify
        the submission process. The default method returns None.

        :param job: The signac job handle.
        :type job: :class:`~signac.contrib.job.Job`
        :returns: The name of the operation to execute next.
        :rtype: str"""
        return

    def eligible(self, job_operation, **kwargs):
        """Determine if job is eligible for operation."""
        return None
