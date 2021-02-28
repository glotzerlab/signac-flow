# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Definition of base classes for the scheduling system."""
import enum
import subprocess
import tempfile
import time
from abc import ABC, abstractmethod

from ..errors import SubmitError


class JobStatus(enum.IntEnum):
    """Classifies the job's execution status."""

    unknown = 1
    """Unknown cluster job status."""

    registered = 2
    """The cluster job is registered with the scheduler, but no other status is known."""

    inactive = 3
    """The cluster job is inactive.

    This includes states like completed, cancelled, or timed out.
    """

    submitted = 4
    """The cluster job has been submitted.

    Note that this state is never returned by a scheduler, but is an assumed
    state immediately after a cluster job is submitted.
    """

    held = 5
    """The cluster job is held."""

    queued = 6
    """The cluster job is queued."""

    active = 7
    """The cluster job is actively running."""

    error = 8
    """The cluster job is in an error or failed state."""

    placeholder = 127
    """A placeholder state that is used for status rendering when no operations are eligible."""

    user = 128
    """All user-defined states must be >=128 in value."""


class ClusterJob:
    """Class representing a cluster job."""

    def __init__(self, job_id, status=None):
        self._job_id = job_id
        self._status = status

    def _id(self):
        return self._job_id

    def __str__(self):
        """Return job ID string."""
        return str(self._id())

    def name(self):
        """Return the name of the cluster job."""
        return self._id()

    def status(self):
        """Return the status of the cluster job."""
        return self._status


class Scheduler(ABC):
    """Abstract base class for schedulers."""

    # The UNIX time stamp of the last scheduler query.
    _last_query = None

    # The amount of time in seconds to wait between scheduler queries.
    # Repeated scheduler queries might risk a denial-of-service attack.
    _dos_timeout = 10

    @classmethod
    def _prevent_dos(cls):
        """Prevent denial of service by enforcing a back-off period.

        This method should *always* be called before querying the scheduler.

        This method will raise an exception if it is called more than
        once within a time window defined by the value of ``_dos_timeout``.
        This is to prevent an (accidental) denial-of-service attack on the
        scheduling system.
        """
        if cls._last_query is not None:
            if time.time() - cls._last_query < cls._dos_timeout:
                raise RuntimeError("Too many scheduler requests within a short time!")
        cls._last_query = time.time()

    @abstractmethod
    def jobs(self):
        """Yield all cluster jobs.

        Yields
        ------
        :class:`.ClusterJob`
            Cluster job.

        """
        raise NotImplementedError()

    @abstractmethod
    def submit(self, script, **kwargs):
        """Submit a job script to the scheduler for execution."""
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def is_present(cls):
        """Return True if the scheduler is detected."""
        raise NotImplementedError()


def _call_submit(submit_cmd, script, pretend):
    """Call submit command with a temporary script file.

    Parameters
    ----------
    submit_cmd : list[str]
        List of strings composing the submission command and any flags.
    script : str
        Script as a string.
    pretend : bool
        If True, the script will be printed to screen instead of submitted.

    Returns
    -------
    bool
        True if the submission command succeeds (or in pretend mode).

    Raises
    ------
    :class:`~flow.errors.SubmitError`
        If the submission command fails.

    """
    submit_cmd_string = " ".join(submit_cmd)
    if pretend:
        print(f"# Submit command: {submit_cmd_string}")
        print(script)
        print()
    else:
        with tempfile.NamedTemporaryFile() as tmp_submit_script:
            tmp_submit_script.write(str(script).encode("utf-8"))
            tmp_submit_script.flush()
            submit_cmd.append(tmp_submit_script.name)
            try:
                subprocess.check_output(
                    submit_cmd, stderr=subprocess.STDOUT, universal_newlines=True
                )
            except subprocess.CalledProcessError as error:
                raise SubmitError(
                    f"Error when calling submission command {submit_cmd_string}:\n{error.output}"
                )

    return True
