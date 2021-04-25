# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for PBS schedulers.

This module implements the Scheduler and ClusterJob classes for PBS.
"""
import errno
import getpass
import io
import logging
import subprocess
import xml.etree.ElementTree as ET

from .base import ClusterJob, JobStatus, Scheduler, _call_submit

logger = logging.getLogger(__name__)


def _fetch(user=None):
    """Fetch the cluster job status information from the PBS scheduler.

    Parameters
    ----------
    user : str
        Limit the status information to cluster jobs submitted by user.
        (Default value = None)

    Yields
    ------
    :class:`~.PBSJob`
        PBS cluster job.

    """
    if user is None:
        user = getpass.getuser()
    cmd = f"qstat -fx -u {user}"
    try:
        result = io.BytesIO(subprocess.check_output(cmd.split()))
        tree = ET.parse(source=result)
        return tree.getroot()
    except ET.ParseError as error:
        if str(error) == "no element found: line 1, column 0":
            logger.warning(
                "No scheduler jobs, from any user(s), were detected. "
                "This may be the result of a misconfiguration in the "
                "environment."
            )
            # Return empty, but well-formed result:
            return ET.parse(source=io.BytesIO(b"<Data></Data>"))
        else:
            raise
    except OSError as error:
        if error.errno == errno.ENOENT:
            raise RuntimeError("PBS not available.")
        else:
            raise error


class PBSJob(ClusterJob):
    """Implementation of the abstract ClusterJob class for PBS schedulers."""

    def __init__(self, node):
        self.node = node

    def _id(self):
        return self.node.find("Job_Id").text

    def status(self):
        """Return the status of the cluster job."""
        job_state = self.node.find("job_state").text
        if job_state == "R":
            return JobStatus.active
        if job_state == "Q":
            return JobStatus.queued
        if job_state == "C":
            return JobStatus.inactive
        if job_state == "H":
            return JobStatus.held
        return JobStatus.registered


class PBSScheduler(Scheduler):
    r"""Implementation of the abstract Scheduler class for PBS schedulers.

    This class can submit cluster jobs to a PBS scheduler and query their
    current status.

    Parameters
    ----------
    user : str
        Limit the status information to cluster jobs submitted by user.
    \*\*kwargs
        Forwarded to the parent constructor.

    """

    # The standard command used to submit jobs to the PBS scheduler.
    submit_cmd = ["qsub"]

    def __init__(self, user=None):
        self.user = user

    def jobs(self):
        """Yield cluster jobs by querying the scheduler."""
        self._prevent_dos()
        nodes = _fetch(user=self.user)
        for node in nodes.findall("Job"):
            yield PBSJob(node)

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
            submission.  Can be used to test whether the submission would be
            successful.  Please note: A successful "pretend" submission is not
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
            submit_cmd.extend(["-W", 'depend="afterok:{}"'.format(after.split(".")[0])])

        if hold:
            submit_cmd += ["-h"]

        return _call_submit(submit_cmd, script, pretend)

    @classmethod
    def is_present(cls):
        """Return True if a PBS scheduler is detected."""
        try:
            subprocess.check_output(["qsub", "--version"], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            return True
        except OSError:
            return False
        else:
            return True
