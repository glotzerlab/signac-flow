# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for LSF schedulers.

This module implements the Scheduler and ClusterJob classes for LSF.
"""
import errno
import getpass
import json
import logging
import subprocess

from .base import ClusterJob, JobStatus, Scheduler, _call_submit

logger = logging.getLogger(__name__)


def _parse_status(s):
    if s in ["PEND", "WAIT"]:
        return JobStatus.queued
    elif s == "RUN":
        return JobStatus.active
    elif s in ["SSUSP", "USUSP", "PSUSP"]:
        return JobStatus.held
    elif s == "DONE":
        return JobStatus.inactive
    elif s == "EXIT":
        return JobStatus.error
    return JobStatus.registered


def _fetch(user=None):
    """Fetch the cluster job status information from the LSF scheduler.

    Parameters
    ----------
    user : str
        Limit the status information to cluster jobs submitted by user.
        (Default value = None)

    Yields
    ------
    :class:`~.LSFJob`
        LSF cluster job.

    """
    if user is None:
        user = getpass.getuser()

    cmd = ["bjobs", "-json", "-u", user]
    try:
        result = json.loads(subprocess.check_output(cmd).decode("utf-8"))
    except subprocess.CalledProcessError:
        raise
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise
        else:
            raise RuntimeError("LSF not available.")
    except json.decoder.JSONDecodeError:
        raise RuntimeError("Could not parse LSF JSON output.")

    for record in result["RECORDS"]:
        yield LSFJob(record)


class LSFJob(ClusterJob):
    """An LSFJob is a ClusterJob managed by an LSF scheduler."""

    def __init__(self, record):
        self.record = record
        self._job_id = record["JOBID"]
        self._status = _parse_status(record["STAT"])

    def name(self):
        """Return the name of the cluster job."""
        return self.record["JOB_NAME"]


class LSFScheduler(Scheduler):
    r"""Implementation of the abstract Scheduler class for LSF schedulers.

    This class can submit cluster jobs to a LSF scheduler and query their
    current status.

    Parameters
    ----------
    user : str
        Limit the status information to cluster jobs submitted by user.
    \*\*kwargs
        Forwarded to the parent constructor.

    """

    # The standard command used to submit jobs to the LSF scheduler.
    submit_cmd = ["bsub"]

    def __init__(self, user=None):
        self.user = user

    def jobs(self):
        """Yield cluster jobs by querying the scheduler."""
        self._prevent_dos()
        yield from _fetch(user=self.user)

    def submit(
        self, script, *, after=None, hold=False, pretend=False, flags=None, **kwargs
    ):
        r"""Submit a job script for execution to the scheduler.

        Parameters
        ----------
        script : str
            The job script submitted for execution.
        after : str
            Execute the submitted script after a job with this id has
            completed. (Default value = None)
        hold : bool
            Whether to hold the job upon submission. (Default value = False)
        pretend : bool
            If True, do not actually submit the script, but only simulate the
            submission. Can be used to test whether the submission would be
            successful. Please note: A successful "pretend" submission is not
            guaranteed to succeed. (Default value = False)
        flags : list
            Additional arguments to pass through to the scheduler submission
            command. (Default value = None)
        \*\*kwargs
            Additional keyword arguments (ignored).

        Returns
        -------
        bool
            True if the submission command succeeds (or in pretend mode).

        Raises
        ------
        :class:`~flow.errors.SubmitError`
            If the submission command fails.

        """
        if flags is None:
            flags = []
        elif isinstance(flags, str):
            flags = flags.split()

        submit_cmd = self.submit_cmd + flags

        if after is not None:
            submit_cmd.extend(["-w", '"done({})"'.format(after.split(".")[0])])

        if hold:
            submit_cmd += ["-H"]

        return _call_submit(submit_cmd, script, pretend)

    @classmethod
    def is_present(cls):
        """Return True if an LSF scheduler is detected."""
        try:
            subprocess.check_output(["bjobs", "-V"], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            return True
        except OSError:
            return False
        else:
            return True
