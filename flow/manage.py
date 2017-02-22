#!/usr/bin/env python3
# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import logging
import enum
import time


logger = logging.getLogger(__name__)


class JobStatus(enum.IntEnum):
    """Classifies the job's execution status.

    The stati are ordered by the significance
    of the execution status.
    This enables easy comparison, such as

    .. code-block: python

        if status < JobStatus.submitted:
            submit()

    which prevents a submission of a job,
    which is already submitted, queued, active
    or in an error state."""
    unknown = 1
    registered = 2
    inactive = 3
    submitted = 4
    held = 5
    queued = 6
    active = 7
    error = 8
# User stati are >= 128.
    user = 128


class ClusterJob(object):

    def __init__(self, jobid, status=None):
        self._job_id = jobid
        self._status = status

    def _id(self):
        return self._job_id

    def __str__(self):
        return str(self._id())

    def name(self):
        return self._id()

    def status(self):
        return self._status


class Scheduler(object):
    "Generic Scheduler ABC"
    _last_query = None
    _dos_timeout = 10

    def __init__(self,
                 header=None, cores_per_node=None,  # legacy arguments
                 *args, **kwargs):
        self.header = header
        self.cores_per_node = cores_per_node

    @classmethod
    def _prevent_dos(cls):
        if cls._last_query is not None:
            if time.time() - cls._last_query < cls._dos_timeout:
                raise RuntimeError(
                    "Too many scheduler requests within a short time!")
        cls._last_query = time.time()

    def jobs(self):
        "yields ClusterJob"
        raise NotImplementedError()


def _status_local(jobsid):
    """Attempt to determine status with local information."""
    return JobStatus.unknown


def _status_scheduler(jobsid, scheduler_jobs):
    """Attempt to determine status with information from the scheduler."""
    cjobs = scheduler_jobs.get(jobsid)
    if cjobs is None:
        status = JobStatus.unknown
    else:
        status = JobStatus.registered
        for cjob in cjobs:
            status = max(status, cjob.status())
    return status


def update_status(job, scheduler_jobs=None):
    """Update the job's status dictionary."""
    status_doc = job.document.setdefault('status', dict())
    for jobsid in status_doc.keys():
        status = _status_local(jobsid)
        if scheduler_jobs is not None:
            status = max(_status_scheduler(jobsid, scheduler_jobs), status)
        status_doc[jobsid] = int(status)
        job.document['status'] = status_doc


def submit(env, project, state_point, script,
           identifier='default', force=False, pretend=False,
           *args, **kwargs):
    """Attempt to submit a job to the scheduler of the current environment.

    The job status will be determined from the job's status document.
    If the job's status is greater or equal than JobStatus.submitted,
    the job will not be submitted, unless the force option is provided."""
    job = project.open_job(state_point)
    # A jobsid is a unique identifier used to identify this job with
    # the job scheduler.
    jobsid = '{}-{}-{}'.format(project, job, identifier)
    logger.info("Attempting submittal of job '{}'.".format(job))
    logger.debug("Determine status...")

    def set_status(value):
        "Update the job's status dictionary."
        status_doc = job.document.get('status', dict())
        status_doc[jobsid] = int(value)
        job.document['status'] = status_doc
    try:
        status = job.document['status'][jobsid]
    except KeyError:
        set_status(JobStatus.registered)
        status = job.document['status'][jobsid]
    if not force:
        if status >= JobStatus.submitted:
            logger.info(
                "Job currently blocked from submission "
                "(already submitted or active).")
            return False
    try:
        assert pretend or env.submit(
            jobsid=jobsid, script=script, pretend=pretend, *args, **kwargs)
    except Exception:
        logger.warning("Error.")
        set_status(JobStatus.error)
        raise
    else:
        set_status(JobStatus.submitted)
        logger.info("Success.")
        return True
