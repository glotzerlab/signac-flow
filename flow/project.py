# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import sys
import os
import io
import logging
import datetime
import json
from collections import defaultdict
from itertools import islice
from hashlib import sha1

import signac

from . import manage
from . import util
from .util.tqdm import tqdm


logger = logging.getLogger(__name__)

DEFAULT_WALLTIME_HRS = 12


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
abbreviate.table = dict()


def shorten(x, max_length=-1):
    return abbreviate(x, x[:max_length])


def _update_status(args):
    return manage.update_status(* args)


class JobScript(io.StringIO):
    "Simple StringIO wrapper to implement cmd wrapping logic."

    def writeline(self, line, eol='\n'):
        "Write one line to the job script."
        self.write(line + eol)

    def write_cmd(self, cmd, parallel=False, np=1, mpi_cmd=None, **kwargs):
        """Write a command to the jobscript.

        This command wrapper function is a convenience function, which
        adds mpi and other directives whenever necessary.

        The ``mpi_cmd`` argument should be a callable, with the following
        signature: ``mpi_cmd(cmd, np, **kwargs)``.

        :param cmd: The command to write to the jobscript.
        :type cmd: str
        :param parallel: Commands should be executed in parallel.
        :type parallel: bool
        :param np: The number of processors required for execution.
        :type np: int
        :param mpi_cmd: MPI command wrapper.
        :type mpi_cmd: callable
        :param kwargs: All other forwarded parameters."""
        if np > 1:
            if mpi_cmd is None:
                raise RuntimeError("Requires mpi_cmd wrapper.")
            cmd = mpi_cmd(cmd, np=np)
        if parallel:
            cmd += ' &'
        self.writeline(cmd)
        return np


class FlowProject(signac.contrib.Project):
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

    def _get_jobsid(self, job, operation):
        "Return a unique job session id based on the job and operation."
        return '{jobid}-{operation}'.format(jobid=job, operation=operation)

    def _store_bundled(self, job_ids):
        """Store all job session ids part of one bundle.

        The job session ids are stored in a text file in the project's
        root directory. This is necessary to be able to identify each
        job's individual status from the bundle id."""
        sid = '{self}-bundle-{sid}'.format(
            self=self,
            sid=sha1('.'.join(job_ids).encode('utf-8')).hexdigest())
        with open(os.path.join(self.root_directory(), sid), 'w') as file:
            for job_id in job_ids:
                file.write(job_id + '\n')
        return sid

    def _expand_bundled_jobs(self, scheduler_jobs):
        "Expand jobs which were submitted as part of a bundle."
        for job in scheduler_jobs:
            if job.name().startswith('{}-bundle-'.format(self)):
                fn_bundle = os.path.join(self.root_directory(), job.name())
                with open(fn_bundle) as file:
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

    def _blocked(self, job, operation, **kwargs):
        "Check if job, operation combination is blocked for scheduling."
        try:
            status = job.document['status'][self._get_jobsid(job, operation)]
            return status >= manage.JobStatus.submitted
        except KeyError:
            return False

    def _eligible(self, job, operation=None, **kwargs):
        """Internal check for the job's eligible for operation.

        A job is only eligible if the public :meth:`~.eligible` method
        returns True and the job is not blocked by the scheduler.

        :raises RuntimeError: If the public eligible method returns None."""
        ret = self.eligible(job, operation, **kwargs) \
            and not self._blocked(job, operation, **kwargs)
        if ret is None:
            raise RuntimeError("Unable to determine eligiblity for job '{}' "
                               "and job type '{}'.".format(job, operation))
        return ret

    def _submit(self, scheduler, to_submit, pretend,
                serial, bundle, after, walltime, **kwargs):
        "Submit jobs to the scheduler."
        script = JobScript()
        self.write_header(
            script=script, walltime=walltime, serial=serial,
            bundle=bundle, after=after, ** kwargs)
        jobids_bundled = []
        np_total = 0
        for job, operation in to_submit:
            jobsid = self._get_jobsid(job, operation)

            def set_status(value):
                "Update the job's status dictionary."
                status_doc = job.document.get('status', dict())
                status_doc[jobsid] = int(value)
                job.document['status'] = status_doc
                return int(value)

            np = self.write_user(
                script=script, job=job, operation=operation,
                parallel=not serial and bundle is not None, **kwargs)
            if np is None:
                raise RuntimeError(
                    "Failed to return 'num_procs' value in write_user()!")
            np_total = max(np, np_total) if serial else np_total + np
            if pretend:
                set_status(manage.JobStatus.registered)
            else:
                set_status(manage.JobStatus.submitted)
            jobids_bundled.append(jobsid)
        script.write('wait')
        script.seek(0)
        if not len(jobids_bundled):
            return False

        if len(jobids_bundled) > 1:
            sid = self._store_bundled(jobids_bundled)
        else:
            sid = jobsid
        scheduler_job_id = scheduler.submit(
            script=script, jobsid=sid,
            np=np_total, walltime=walltime, pretend=pretend, **kwargs)
        logger.info("Submitted {}.".format(sid))
        if serial and not bundle:
            if after is None:
                after = ''
            after = ':'.join(after.split(':') + [scheduler_job_id])
        return True

    def to_submit(self, job_ids=None, operation=None, job_filter=None):
        """Generate a sequence of (job_id, operation) value pairs for submission.

        :param job_ids: A list of job_id's,
            defaults to all jobs found in the workspace.
        :param operation: A specific operation,
            defaults to the result of :meth:`~.next_operation`.
        :param job_filter: A JSON encoded filter,
            that all jobs to be submitted need to match."""
        if job_ids is None:
            jobs = list(self.find_jobs(job_filter))
        else:
            jobs = [self.open_job(id=jobid) for jobid in job_ids]
        if operation is None:
            operations = (self.next_operation(job) for job in jobs)
        else:
            operations = [operation] * len(jobs)
        return zip(jobs, operations)

    def filter_non_eligible(self, to_submit, **kwargs):
        "Return only those jobs for submittal, which are eligible."
        return ((j, jt) for j, jt in to_submit
                if self._eligible(j, jt, **kwargs))

    def submit_jobs(self, scheduler, to_submit, walltime=None,
                    bundle=None, serial=False, after=None,
                    num=None, pretend=False, force=False, **kwargs):
        """Submit jobs to the scheduler.

        :param scheduler: The scheduler instance.
        :type scheduler: :class:`~.flow.manage.Scheduler`
        :param to_submit: A sequence of (job_id, operation) tuples.
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
        if walltime is not None:
            walltime = datetime.timedelta(hours=walltime)
        if not force:
            to_submit = self.filter_non_eligible(to_submit, **kwargs)
        to_submit = islice(to_submit, num)
        if bundle is not None:
            n = None if bundle == 0 else bundle
            while True:
                ts = islice(to_submit, n)
                if not self._submit(scheduler, ts, walltime=walltime,
                                    bundle=bundle, serial=serial, after=after,
                                    num=num, pretend=pretend, force=force, **kwargs):
                    break
        else:
            for ts in to_submit:
                self._submit(scheduler, [ts], walltime=walltime,
                             bundle=bundle, serial=serial, after=after,
                             num=num, pretend=pretend, force=force, **kwargs)

    def submit(self, scheduler, job_ids=None,
               operation=None, job_filter=None, **kwargs):
        """Wrapper for :meth:`~.to_submit` and :meth:`~.submit_jobs`.

        This function passes the return value of :meth:`~.to_submit`
        to :meth:`~.submit_jobs`.

        :param scheduler: The scheduler instance.
        :type scheduler: :class:`~.flow.manage.Scheduler`
        :param job_ids: A list of job_id's,
            defaults to all jobs found in the workspace.
        :param operation: A specific operation,
            defaults to the result of :meth:`~.next_operation`.
        :param job_filter: A JSON encoded filter that all jobs
            to be submitted need to match.
        :param kwargs: All other keyword arguments are forwarded
            to :meth:`~.submit_jobs`."""
        if job_ids is not None and not len(job_ids):
            job_ids = None
        return self.submit_jobs(
            scheduler=scheduler,
            to_submit=self.to_submit(job_ids, operation, job_filter),
            **kwargs)

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
            dest='operation',
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
            '-f', '--filter',
            dest='job_filter',
            type=str,
            help="Filter jobs.")
        parser.add_argument(
            '--after',
            type=str,
            help="Schedule this job to be executed after "
                 "completion of job with this id.")
        parser.add_argument(
            '-s', '--serial',
            action='store_true',
            help="Schedule the jobs to be executed serially.")
        parser.add_argument(
            '--bundle',
            type=int,
            nargs='?',
            const=0,
            help="Specify how many jobs to bundle into one submission. "
                 "Omit a specific value to bundle all eligible jobs.")
        parser.add_argument(
            '--hold',
            action='store_true',
            help="Submit job with user hold applied.")

    def write_human_readable_statepoint(self, script, job):
        "Write statepoint of job in human-readable format to script."
        script.write('# Statepoint:\n#\n')
        sp_dump = json.dumps(job.statepoint(), indent=2).replace(
            '{', '{{').replace('}', '}}')
        for line in sp_dump.splitlines():
            script.write('# ' + line + '\n')
        script.write('\n')

    def write_header(self, script, walltime, env=None, **kwargs):
        """Write a general jobscript header to the script.

        The header is written only once for each job script, whereas the
        output of :meth:`~.write_user` may be written multiple times to
        one job script for bundled jobs.

        :param script: The job script, to write to.
        :type script: :class:`~.JobScript`
        :param walltime: The maximum allowed walltime for this operation.
        :type walltime: :class:`datetime.timedelta`

        The default method writes nothing."""
        return

    def write_user(self, script, job, operation, parallel, **kwargs):
        """Write to the jobscript for job and job type."

        This function needs to be specialized for each project.
        Please note, that all commands should obey the parallel flag, i.e.
        should be executed in parallel if set to True.
        You should `assert not parallel` if that is not possible.

        See also: :meth:`~.JobScript.write_cmd`.

        The ``mpi_cmd`` argument should be a callable, with the following
        signature: ``mpi_cmd(cmd, np, **kwargs)``.

        :param script: The job script, to write to.
        :type script: :class:`~.JobScript`
        :param job: The signac job handle.
        :type job: :class:`signac.contrib.Job`
        :param operation: The name of the operation to execute.
        :type operation: str
        :param parallel: Execute commands in parallel if True.
        :type parallel: bool
        :param mpi_cmd: MPI command wrapper. Pass this function
            to execute mpi commands in multiple environments.

        :returns: The number of required processors (nodes).
        :rtype: int
        """
        raise NotImplementedError()

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

    def format_row(self, status, statepoint=None, max_width=-1):
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
                       skip_active=False, param_max_width=-1,
                       use_colors=True,
                       file=sys.stdout):
        "Print the project's detailed status."
        table_header = [self._tr(self._alias(s)) for s in ('job_id', 'status', 'next_operation', 'labels')]
        if parameters:
            for i, value in enumerate(parameters):
                table_header.insert(i + 3, shorten(self._alias(str(value)), param_max_width))
        rows = (self.format_row(status, parameters, param_max_width)
                for status in stati if not (skip_active and status['active']))
        print(util.tabulate.tabulate(rows, headers=table_header), file=file)
        if abbreviate.table:
            print()
            print(self._tr("Abbreviations used:"), file=file)
            for a in sorted(abbreviate.table):
                print('{}: {}'.format(a, abbreviate.table[a]))

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
                     param_max_width=-1,
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
            self.print_overview(stati, max_lines=overview_max_lines)
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
            default=-1,
            help="Limit the width of each parameter row.")
        parser.add_argument(
            '--skip-active',
            action='store_true',
            help="Display only jobs, which are currently not active.")

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

    def eligible(self, job, operation=None, **kwargs):
        """Determine if job is eligible for operation."""
        return None
