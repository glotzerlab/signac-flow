#!/usr/bin/env python
"""Submit jobs to the configured scheduler.

This module will iterate over all jobs defined in your
workspace, determine the next operation and attempt to
submit the job.

No job will be submitted if another job is already
submitted, queued or active."""
import sys
import os
import io
import logging
import warnings
import datetime
import argparse
import json
import copy
from itertools import islice
from hashlib import sha1

import signac
import flow

import environment
import jobs as jobdefs


logger = logging.getLogger(__name__)

ENV = flow.environment.get_environment()
DEFAULT_WALLTIME_HRS = 12

project = signac.get_project()


def write_human_readable_statepoint(script, job):
    "Write statepoint in human-readable format to script."
    script.write('# Statepoint:\n#\n')
    sp_dump = json.dumps(job.statepoint(), indent=2).replace(
        '{', '{{').replace('}', '}}')
    for line in sp_dump.splitlines():
        script.write('# ' + line + '\n')
    script.write('\n')


def create_script(max_walltime, walltime_buffer=None):
    # Set default walltime buffer if necessary
    if walltime_buffer is None:
        walltime_buffer = datetime.timedelta(minutes=5)

    # Create submit script
    script = io.StringIO()

    # Write hoomd-walltime stop to script.
    # (Does not harm even if we don't actually use hoomd-blue.)
    script.write('export HOOMD_WALLTIME_STOP=$((`date +%s` + {}))\n'.format(
        (max_walltime - walltime_buffer).seconds))

    # Enter the project's root directory before executing any command.
    script.write('cd {root}\n'.format(root=project.root_directory()))
    return script


def store_bundled(job_ids):
    sid = '{project}-bundle-{sid}'.format(
        project=project,
        sid=sha1('.'.join(job_ids).encode('utf-8')).hexdigest())
    with open(os.path.join(project.root_directory(), sid), 'w') as file:
        for job_id in job_ids:
            file.write(job_id + '\n')
    return sid


def submit(jobs, args):
    if args.minutes:
        walltime = datetime.timedelta(minutes=args.walltime)
    else:
        walltime = datetime.timedelta(hours=args.walltime)
    if args.minutes:
        walltime_buffer = datetime.timedelta(
            minutes=max(0, min(args.walltime - 1, 5)))
    else:
        walltime_buffer = datetime.timedelta(minutes=10)

    script = create_script(walltime, walltime_buffer)
    jobids_bundled = []
    np_total = 0
    for job in jobs:
        next_job = jobdefs.next_job(job)
        jobsid = get_jobsid(job, next_job)

        def set_status(value):
            "Update the job's status dictionary."
            status_doc = job.document.get('status', dict())
            status_doc[jobsid] = int(value)
            job.document['status'] = status_doc
            return int(value)
        write_human_readable_statepoint(script, job)
        np = write_user(script, job, next_job, args)
        np_total = max(np, np_total) if args.serial else np_total + np
        if args.pretend:
            set_status(flow.manage.JobStatus.registered)
        else:
            set_status(flow.manage.JobStatus.submitted)
        jobids_bundled.append(jobsid)
    script.write('wait')
    script.seek(0)
    if not len(jobids_bundled):
        return False

    if len(jobids_bundled) > 1:
        sid = store_bundled(jobids_bundled)
    else:
        sid = jobsid
    scheduler = environment.get_scheduler(ENV, mode='gpu' if args.gpu else 'cpu')
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


def write_user(script, job, next_job, args):
    np = len(job.statepoint()['states'])
    cmd = 'hoomd scripts/run.py {jobname} {jobid}'
    if args.bundle is not None and not args.serial:
        cmd += ' &'
    script.write(ENV.mpi_cmd(cmd.format(
        jobname=next_job, jobid=job)).format(np=np))
    return np


def get_jobsid(job, next_job):
    return '{jobid}-{name}'.format(jobid=job, name=next_job)


def blocked(job, next_job):
    try:
        status = job.document['status'][get_jobsid(job, next_job)]
        return status >= flow.manage.JobStatus.submitted
    except KeyError:
        return False


def eligible(jobs, target=None, gpu=False):
    for job in jobs:
        next_job = jobdefs.next_job(job)
        if next_job is None:
            logger.debug("{}: Nothing to be done.".format(job))
            continue
        if not (gpu == (next_job in ('converge', ))):
            continue
        if blocked(job, next_job):
            continue
        if target is None or next_job == target:
            yield job


def main(args):
    if args.filter and args.jobid:
        raise ValueError("Can't filter argument with specified job ids.")
    logger.info("Environment: {}".format(ENV))
    filter = None if args.filter is None else eval(args.filter)
    if args.jobid:
        jobs = (project.open_job(id=jobid) for jobid in args.jobid)
    else:
        jobs = project.find_jobs(filter)
    if not args.force:
        jobs = eligible(jobs, args.job, args.gpu)
    jobs = islice(jobs, args.num)
    if args.bundle is not None:
        while(submit(
                islice(jobs, None if args.bundle == 0 else args.bundle),
                args)):
                pass
    else:
        for job in jobs:
            submit([job], args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Submit jobs to compute cluster.")
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

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    sys.exit(main(args))
