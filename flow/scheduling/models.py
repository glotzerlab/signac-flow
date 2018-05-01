# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import enum
import time


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
