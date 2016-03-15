import sys
import os
import io
import logging
import datetime
import json
import multiprocessing
from collections import defaultdict
from itertools import islice
from hashlib import sha1

import signac

from . import manage
from . import util
from .util.tqdm import tqdm


logger = logging.getLogger(__name__)

DEFAULT_WALLTIME_HRS = 12


class BasicFlowProject(signac.contrib.Project):

    pass


class FlowProject(BasicFlowProject):

    def is_active(self, status):
        for gid, s in status.items():
            if s > manage.JobStatus.inactive:
                return True
        return False

    def create_script(self, max_walltime, walltime_buffer=None):
        # Set default walltime buffer if necessary
        if walltime_buffer is None:
            walltime_buffer = datetime.timedelta(minutes=5)

        # Create submit script
        script = io.StringIO()

        # Write hoomd-walltime stop to script.
        # (Does not harm even if we don't actually use hoomd-blue.)
        script.write(
            'export HOOMD_WALLTIME_STOP=$((`date +%s` + {}))\n'.format(
                (max_walltime - walltime_buffer).total_seconds()))

        # Enter the self's root directory before executing any command.
        script.write('cd {root}\n'.format(root=self.root_directory()))
        return script

    def store_bundled(self, job_ids):
        sid = '{self}-bundle-{sid}'.format(
            self=self,
            sid=sha1('.'.join(job_ids).encode('utf-8')).hexdigest())
        with open(os.path.join(self.root_directory(), sid), 'w') as file:
            for job_id in job_ids:
                file.write(job_id + '\n')
        return sid

    def expand_bundled_jobs(self, scheduler_jobs):
        # Expand jobs which were submitted as part of a bundle
        for job in scheduler_jobs:
            if job.name().startswith('{}-bundle-'.format(self)):
                fn_bundle = os.path.join(self.root_directory(), job.name())
                with open(fn_bundle) as file:
                    for line in file:
                        yield manage.ClusterJob(line.strip(), job.status())
            else:
                yield job

    def _submit(self, env, jobs, jobnames, args):
        assert len(jobs) == len(jobnames)
        if args.minutes:
            walltime = datetime.timedelta(minutes=args.walltime)
        else:
            walltime = datetime.timedelta(hours=args.walltime)
        if args.minutes:
            walltime_buffer = datetime.timedelta(
                minutes=max(0, min(args.walltime - 1, 5)))
        else:
            walltime_buffer = datetime.timedelta(minutes=10)

        script = io.StringIO()
        self.write_header(script, walltime, walltime_buffer)
        jobids_bundled = []
        np_total = 0
        for job, jobname in zip(jobs, jobnames):
            jobsid = self.get_jobsid(job, jobname)

            def set_status(value):
                "Update the job's status dictionary."
                status_doc = job.document.get('status', dict())
                status_doc[jobsid] = int(value)
                job.document['status'] = status_doc
                return int(value)
            np = self.write_user(env, script, job, jobname, args)
            np_total = max(np, np_total) if args.serial else np_total + np
            if args.pretend:
                set_status(manage.JobStatus.registered)
            else:
                set_status(manage.JobStatus.submitted)
            jobids_bundled.append(jobsid)
        script.write('wait')
        script.seek(0)
        if not len(jobids_bundled):
            return False

        if len(jobids_bundled) > 1:
            sid = self.store_bundled(jobids_bundled)
        else:
            sid = jobsid
        scheduler = env.get_scheduler(
            env, mode='gpu' if args.gpu else 'cpu')
        scheduler_job_id = scheduler.submit(
            script=script, jobsid=sid,
            np=np_total, walltime=walltime, pretend=args.pretend,
            force=args.force, after=args.after, hold=args.hold)
        logger.info("Submitted {}.".format(sid))
        if args.serial and not args.bundle:
            if args.after is None:
                args.after = ''
            args.after = ':'.join(args.after.split(':') + [scheduler_job_id])
        return True

    def get_jobsid(self, job, jobname):
        return '{jobid}-{name}'.format(jobid=job, name=jobname)

    def submit(self, env, args):
        if args.filter and args.jobid:
            raise ValueError("Can't filter argument with specified job ids.")
        logger.info("Environment: {}".format(env))
        filter = None if args.filter is None else eval(args.filter)
        if args.jobid:
            jobs = (self.open_job(id=jobid) for jobid in args.jobid)
        else:
            jobs = self.find_jobs(filter)
        if not args.force:
            jobs = (job for job in jobs if self.eligible(job, args.job, args.gpu))
        jobs = list(islice(jobs, args.num))
        jobnames = (self.next_job(job) if args.target is None else args.target for job in jobs)
        if args.bundle is not None:
            n = None if args.bundle == 0 else args.bundle
            while(self._submit(islice(jobs, n), islice(jobnames, n), args)):
                pass
        else:
            for job, jobname in zip(jobs, jobnames):
                self._submit([job], [jobname], args)

    def add_submit_parser_arguments(self, parser):
        parser.add_argument(
            'jobid',
            type=str,
            nargs='*',
            help="The job id of the jobs to submit. "
            "Omit to automatically select all eligible jobs.")
        parser.add_argument(
            '-j', '--job',
            type=str,
            help="Limit the the type of jobs to submit.")
        parser.add_argument(
            '-w', '--walltime',
            type=int,
            default=DEFAULT_WALLTIME_HRS,
            help="The wallclock time in hours.")
        parser.add_argument(
            '--minutes',
            action='store_true',
            help="Set the walltime clock in minutes.")
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
            '--gpu',
            action='store_true',
            help="Submit to a gpu queue.")
        parser.add_argument(
            '--hold',
            action='store_true',
            help="Submit job with user hold applied.")

    def blocked(self, job, jobname):
        try:
            status = job.document['status'][self.get_jobsid(job, jobname)]
            return status >= manage.JobStatus.submitted
        except KeyError:
            return False

    def _eligible(self, job, jobname=None):
        return self.eligible(job, jobname) and not self.blocked(job, jobname)

    def write_human_readable_statepoint(self, script, job):
        "Write statepoint in human-readable format to script."
        script.write('# Statepoint:\n#\n')
        sp_dump = json.dumps(job.statepoint(), indent=2).replace(
            '{', '{{').replace('}', '}}')
        for line in sp_dump.splitlines():
            script.write('# ' + line + '\n')
        script.write('\n')

    def write_header(self, script, walltime, walltime_buffer, job=None, jobname=None):
        self.write_human_readable_statepoint(script, job)

        # Write hoomd-walltime stop to script.
        # (Does not harm even if we don't actually use hoomd-blue.)
        script.write('export HOOMD_WALLTIME_STOP=$((`date +%s` + {}))\n'.format(
            (walltime - walltime_buffer).total_seconds()))

        # Enter the self's root directory before executing any command.
        script.write('cd {root}\n'.format(root=self.root_directory()))

    def write_user(self, env, script, job, jobname, args):
        np = 1
        cmd = 'python scripts/run.py {jobname} {jobid}'.format(
            jobname=jobname, jobid=job)
        if args.bundle is not None and not args.serial:
            cmd += ' &'
        if np > 1:
            cmd = env.mpi_cmd(cmd).format(np=np)
        return np

    def eligible(job, jobname=None, gpu=False):
        return None

    def fetch_job_status(self, args):
        self, job, scheduler_jobs = args
        result = dict()
        result['job_id'] = str(job)
        manage.update_status(job, scheduler_jobs)
        status = job.document.get('status', dict())
        result['active'] = self.is_active(status)
        result['labels'] = sorted(set(self.classify(job)))
        if not result['labels']:
            return
        result['jobname'] = self.next_job(job)
        highest_status = max(status.values()) if len(status) else 1
        result['submission_status'] = [manage.JobStatus(highest_status).name]
        return result

    def format_row(self, status, statepoint=None):
        row = [
            status['job_id'],
            ', '.join(status['submission_status']),
            status['jobname'],
            ', '.join(status.get('labels', [])),
        ]
        if statepoint:
            sps = self.open_job(id=status['job_id']).statepoint()
            for i, sp in enumerate(statepoint):
                row.insert(i + 1, sps.get(sp))
        return row

    def draw_progressbar(self, value, total, width=40):
        n = int(value / total * width)
        return '|' + ''.join(['#'] * n) + ''.join(['-'] * (width - n)) + '|'

    def print_overview(self, stati, file=sys.stdout):
        # determine progress of all labels
        progress = defaultdict(int)
        for status in stati:
            for label in status['labels']:
                progress[label] += 1
        progress_sorted = sorted(
            progress.items(), key=lambda x: (x[1], x[0]), reverse=True)
        table_header = ['label', 'progress']
        rows = ([label, '{} {:0.2f}%'.format(
            self.draw_progressbar(num, len(stati)), 100 * num / len(stati))]
            for label, num in progress_sorted)
        print("Total # of jobs: {}".format(len(stati)), file=file)
        print(util.tabulate.tabulate(rows, headers=table_header), file=file)

    def print_detailed_view(self, stati, statepoint=None, file=sys.stdout):
        table_header = ['job_id', 'status', 'jobname', 'labels']
        if statepoint:
            for i, sp in enumerate(statepoint):
                table_header.insert(i + 1, sp)
        rows = (self.format_row(status, statepoint)
                for status in stati if not
                (args.inactive and status['active']) and
                (status['active'] or status['jobname']))
        print(util.tabulate.tabulate(rows, headers=table_header), file=file)

    def print_status(self, env, args, file=sys.stdout, err=sys.stderr):
        try:
            scheduler = env.get_scheduler()
            scheduler_jobs = {job.name(): job for job in
                              self.expand_bundled_jobs(self, scheduler.jobs())}
        except ImportError as error:
            print("WARNING: Requested scheduler not available. "
                  "Unable to determine batch job status.", file=err)
            print(error, file=err)
            scheduler_jobs = None
        filter_ = None if args.filter is None else eval(args.filter)
        print("Finding all jobs...", file=err)
        N = self.num_jobs()
        if args.cache:
            query = {'statepoint.states': {'$exists': True}}
            docs = signac.get_database('shape_potentials').index.find(query)
            jobs = [self.open_job(id=doc['signac_id'])
                    for doc in tqdm(docs, total=docs.count())]
        else:
            jobs = [job for job in tqdm(islice(self.find_jobs(
                filter_), N), total=N) if 'states' in job.statepoint()]
            print("Determine job stati...", file=err)
        with multiprocessing.Pool() as pool:
            stati = pool.imap_unordered(
                self.fetch_job_status, [(self, job, scheduler_jobs) for job in jobs])
            stati = [status for status in filter(
                None, tqdm(stati, total=len(jobs)))]
        print("Done.", file=err)
        job_dict = {str(job): job for job in jobs}
        if args.export:
            mc = signac.get_database(str(self)).job_stati
            for status in stati:
                status['statepoint'] = job_dict[status['job_id']].statepoint()
                mc.update_one({'_id': status['job_id']}, {
                              '$set': status}, upsert=True)
        title = "Status self '{}':".format(self)
        print('\n' + title, file=file)
        self.print_overview(stati)
        if args.detailed_view:
            print(file=file)
            print("Detailed view:", file=file)
            self.print_detailed_view(stati, args.statepoint, file)

    def add_status_parser_arguments(self, parser):
        parser.add_argument(
            '-f', '--filter',
            type=str,
            help="Filter jobs.")
        parser.add_argument(
            '-d', '--detailed-view',
            action='store_true',
            help="Display a detailed view of the job stati.")
        parser.add_argument(
            '-s', '--statepoint',
            type=str,
            nargs='*',
            help="Display a select parameter of the statepoint "
                 "with the detailed view.")
        parser.add_argument(
            '-i', '--inactive',
            action='store_true',
            help="Display all jobs, that require attention.")
        parser.add_argument(
            '-e', '--export',
            action='store_true',
            help="Export job stati to MongoDB database collection.")
        parser.add_argument(
            '-c', '--cache',
            action='store_true',
            help="Use the cached index for loading jobs.")

    def classify(self, job):
        yield None

    def next_job(self, job):
        return
