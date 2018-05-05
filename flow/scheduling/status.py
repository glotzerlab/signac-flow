# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging

from .base import JobStatus

logger = logging.getLogger(__name__)


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
        if status_doc[jobsid] != int(status):
            status_doc[jobsid] = int(status)
            job.document['status'] = status_doc
