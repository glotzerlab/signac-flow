#!/usr/bin/env python3
# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Routines for the APScheduler environment."""

from __future__ import print_function
import logging
import subprocess
import tempfile
import argparse
import random
import time
from datetime import datetime, timedelta

import signac
import apscheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.triggers.date import DateTrigger

from .mongodb_queue import MongoDBQueue, Empty
from .mongodb_set import MongoDBSet
from .manage import Scheduler, ClusterJob, JobStatus


logger = logging.getLogger(__name__)


def format_timedelta(delta):
    hours, r = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(r, 60)
    hours += delta.days * 24
    return "{:0>2}:{:0>2}:{:0>2}".format(hours, minutes, seconds)


def get_queue(db, name):
    project = signac.get_project()
    q_name = "{project}_scheduler_{name}".format(
        project=project, name=name)
    return MongoDBQueue(db[q_name])
    # return MongoDBQueue(signac.db.get_database(db)[q_name])


def get_set(name, db=None):
    project = signac.get_project()
    if db is None:
        db = str(project)
    q_name = "{project}-scheduler-{name}".format(
        project=project, name=name)
    return MongoDBSet(signac.db.get_database(db)[q_name])


def transfer(item, src, dst):
    get_set(dst).add(item)
    get_set(src).discard(item)


def execute_script(jobsid, np, walltime, script, *args, **kwargs):
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(script.encode())
        tmp.flush()
        with open('{}.log'.format(jobsid), 'ab') as logfile:
            try:
                cmd = '/bin/sh {}'.format(tmp.name)
                logger.debug("Executing command '{}'".format(cmd))
                subprocess.check_call(
                    cmd,
                    stdout=logfile,
                    stderr=logfile,
                    shell=True)
            except subprocess.CalledProcessError as error:
                logger.warning(error)
            finally:
                logfile.flush()


def run_job(scheduler, job_doc):
    try:
        transfer(job_doc['jobsid'], 'queued', 'active')
        execute_script(** job_doc)
    except Exception as error:
        logger.error(error)
        raise
    finally:
        transfer(job_doc['jobsid'], 'active', 'completed')


def submit_job(scheduler, queue, misfire_grace_time, next_job):
    run_date = next_job.get('run_date', datetime.now())
    run_date += timedelta(seconds=10+int(random.random() * 10))
    after = next_job.get('after')
    assert after is None
    trigger = DateTrigger(run_date=run_date)
    scheduler.add_job(
        run_job,
        id=next_job['jobsid'],
        args=(scheduler, next_job),
        #executor='processpool',
        trigger=trigger,
        misfire_grace_time=misfire_grace_time)
    return next_job['jobsid']


def poll_submits(scheduler, queue, misfire_grace_time=None):
    logger.debug("Pulling from submittal queue.")
    # Allow delay of job start up to an hour later to prevent skipping
    if misfire_grace_time is None:
        misfire_grace_time = 3600
    while True:
        try:
            next_job = queue.get(block=False)
            if next_job.get('after') is None:
                submit_job(scheduler, queue, misfire_grace_time, next_job)
            else:
                jobid = 'submitafter-{}'.format(next_job['after'])
                scheduler.add_job(
                    submit_after,
                    id=jobid,
                    args=(jobid, scheduler, queue, misfire_grace_time, next_job),
                    executor='threadpool',
                    trigger='interval',
                    seconds=20)
            get_set('queued').add(next_job['jobsid'])
        except Empty:
            break
        else:
            queue.task_done()
            time.sleep(1)


def submit_after(jobid, scheduler, queue, misfire_grace_time, next_job):
    after = next_job['after']
    if after in get_set('completed'):
        submit_job(scheduler, queue, misfire_grace_time, next_job)
        scheduler.get_job(jobid).remove()


def show_jobs(scheduler):
    print(scheduler.get_jobs())


class APSJob(ClusterJob):
    pass


class APScheduler(Scheduler):

    def __init__(self, db):
        self.scheduler = get_scheduler()
        self.db = db

    def _get_submit_queue(self):
        return get_queue(self.db, 'submitted')

    def jobs(self):
        stati = dict()
        for job in self._get_submit_queue().peek():
            stati[job['jobsid']] = JobStatus.submitted
        for name, status in (
                ('queued', JobStatus.queued),
                ('active', JobStatus.active),
                ('completed', JobStatus.inactive)):
            for job_id in get_set(name):
                stati[job_id] = max(
                    stati.get(job_id, JobStatus.registered), status)
                yield APSJob(job_id, status)
        for job_id, status in stati.items():
            yield APSJob(job_id, status)

    def submit(self, jobsid, np, walltime, script,
               pretend=False, after=None, **kwargs):
        job_doc = {
            'jobsid': jobsid,
            'np': np,
            'walltime': walltime.seconds,
            'after': after,
            'script': script.read().format(
                jobsid=jobsid, np=np, walltime=format_timedelta(walltime)),
        }
        if pretend:
            print(type(self), "Pretend to submit:\n\n", job_doc)
            return jobsid
        else:
            logger.info("Submitting '{}'.".format(job_doc))
            self._get_submit_queue().put(job_doc)
            return jobsid


def get_scheduler(num_procs=1):
    return BlockingScheduler(
        jobstores={
            'mongo': MongoDBJobStore(
                client=signac.common.host.get_client(),
                database=str(signac.get_project()),
                collection='apscheduler')},
        executors={
            'threadpool': ThreadPoolExecutor(),
            #'processpool': ProcessPoolExecutor(max_workers=num_procs)
            },
    )


def clear_sets():
    logger.debug("Clearing internal state.")
    for name in ('queued', 'active', 'completed'):
        get_set(name).clear()


def reset(db):
    logger.info("Resetting all queues.")
    get_queue(db, 'submitted').clear()
    clear_sets()


def start_scheduler(num_procs, db):
    scheduler = get_scheduler(num_procs)
    clear_sets()
    print("Starting scheduler.")
    scheduler.add_job(
        poll_submits,
        args=(scheduler, get_queue(db, 'submitted')),
        executor='threadpool',
        trigger='cron',
        second='*/10')
    # scheduler.add_job(
    #     show_jobs,
    #     args=(scheduler,),
    #     trigger='cron',
    #     second='*/12')
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("Shutting down scheduler. ", end='')
        print("Hit ctrl-c again to exit immediately.")
        scheduler.shutdown(wait=True)
        print("Scheduler shut down.")
    except BaseException:
        scheduler.shutdown(wait=False)
        raise


def main(args):
    db = signac.db.get_database(args.db)
    if args.reset:
        reset(db)
    else:
        start_scheduler(num_procs=args.num_procs, db=db)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Execute a test scheduler. "
                    "Do not start more than one instance!")
    parser.add_argument(
        '-n', '--num-procs',
        type=int,
        default=1,
        help="The number of processors to use for execution.")
    parser.add_argument(
        '--reset',
        action='store_true',
        help="Do not start the scheduler, but reset all internal queues.")
    parser.add_argument(
        '--db',
        type=str,
        default=str(signac.get_project()),
        help="Specify which database to use.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    main(args)
