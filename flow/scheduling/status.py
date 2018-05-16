# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging

from .base import JobStatus

logger = logging.getLogger(__name__)


def _status_local(scheduler_job_id):
    """Attempt to determine status with local information."""
    return JobStatus.unknown


def _status_scheduler(scheduler_job_id, scheduler_jobs):
    """Attempt to determine status with information from the scheduler."""
    cjobs = scheduler_jobs.get(scheduler_job_id)
    if cjobs is None:
        status = JobStatus.unknown
    else:
        status = JobStatus.registered
        for cjob in cjobs:
            status = max(status, cjob.status())
    return status


def update_status(job, scheduler_jobs=None):
    """Update the job's status dictionary."""

    # The status docs maps the scheduler job id to a distinct JobStatus value.
    status_doc = job.document.setdefault('status', dict())

    # Iterate through all entries within the job's status doc:
    for scheduler_job_id in status_doc:
        status = JobStatus.unknown     # default status
        if scheduler_jobs is not None:
            status = max(_status_scheduler(scheduler_job_id, scheduler_jobs), status)
        # Update the status doc, in case that the fetched status differs:
        if status_doc[scheduler_job_id] != int(status):
            status_doc[scheduler_job_id] = int(status)
    # Write back to job document
    job.document['status'] = status_doc
