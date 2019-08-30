# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Definition of base classes for the scheduling system."""
import enum
import time


class JobStatus(enum.IntEnum):
    """Classifies the job's execution status."""
    unknown = 1
    registered = 2
    inactive = 3
    submitted = 4
    held = 5
    queued = 6
    active = 7
    error = 8
    dummy = 127
    # All user statuses are >= 128.
    user = 128


class ClusterJob(object):
    """This class represents a cluster job."""

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
    """Abstract base class for schedulers."""

    # The UNIX time stamp of the last scheduler query.
    _last_query = None

    # The amount of time in seconds a user needs to wait, before we
    # assume that repeated scheduler queries might risk a denial-of-service attack.
    _dos_timeout = 10

    @classmethod
    def _prevent_dos(cls):
        """This method should be called before querying the scheduler.

        If this method will raise an exception if it is called more than
        once within a time window defined by the '_dos_timeout' class.
        This is to prevent an (accidental) denial-of-service attack on
        the scheduling system.
        """
        if cls._last_query is not None:
            if time.time() - cls._last_query < cls._dos_timeout:
                raise RuntimeError(
                    "Too many scheduler requests within a short time!")
        cls._last_query = time.time()

    def jobs(self):
        """Yield all cluster jobs.

        :yields:
            :class:`.ClusterJob`
        """
        raise NotImplementedError()
