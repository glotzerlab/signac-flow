# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for SLURM schedulers.

This module implements the Scheduler and ClusterJob classes for SLURM.
"""
import errno
import getpass
import logging
import subprocess
import tempfile

from ..errors import SubmitError
from .base import ClusterJob, JobStatus, Scheduler

logger = logging.getLogger(__name__)


def _fetch(user=None):
    """Fetch the cluster job status information from the SLURM scheduler.

    Parameters
    ----------
    user : str
        Limit the status information to cluster jobs submitted by user.
        (Default value = None)

    Yields
    ------
    :class:`~.SlurmJob`
        SLURM cluster job.

    """

    def parse_status(s):
        s = s.strip()
        if s == "PD":
            return JobStatus.queued
        elif s == "R":
            return JobStatus.active
        elif s in ["CG", "CD", "CA", "TO"]:
            return JobStatus.inactive
        elif s in ["F", "NF"]:
            return JobStatus.error
        return JobStatus.registered

    if user is None:
        user = getpass.getuser()

    cmd = ["squeue", "-u", user, "-h", "--format=%2t%100j"]
    try:
        result = subprocess.check_output(cmd).decode("utf-8", errors="backslashreplace")
    except subprocess.CalledProcessError:
        raise
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise
        else:
            raise RuntimeError("SLURM not available.")
    lines = result.split("\n")
    for line in lines:
        if line:
            status = line[:2]
            name = line[2:].rstrip()
            yield SlurmJob(name, parse_status(status))


class SlurmJob(ClusterJob):
    """A SlurmJob is a ClusterJob managed by a SLURM scheduler."""

    pass


class SlurmScheduler(Scheduler):
    r"""Implementation of the abstract Scheduler class for SLURM schedulers.

    This class can submit cluster jobs to a SLURM scheduler and query their
    current status.

    Parameters
    ----------
    user : str
        Limit the status information to cluster jobs submitted by user.
    \*\*kwargs
        Forwarded to the parent constructor.

    """

    # The standard command used to submit jobs to the SLURM scheduler.
    submit_cmd = ["sbatch"]

    def __init__(self, user=None, **kwargs):
        super().__init__(**kwargs)
        self.user = user

    def jobs(self):
        """Yield cluster jobs by querying the scheduler."""
        self._prevent_dos()
        yield from _fetch(user=self.user)

    def submit(
        self, script, after=None, hold=False, pretend=False, flags=None, **kwargs
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
            Returns True if the cluster job was successfully submitted,
            otherwise None.

        """
        if flags is None:
            flags = []
        elif isinstance(flags, str):
            flags = flags.split()

        submit_cmd = self.submit_cmd + flags

        if after is not None:
            submit_cmd.extend(
                ["-W", 'depend="afterany:{}"'.format(after.split(".")[0])]
            )

        if hold:
            submit_cmd += ["--hold"]

        if pretend:
            print("# Submit command: {}".format("  ".join(submit_cmd)))
            print(script)
            print()
        else:
            with tempfile.NamedTemporaryFile() as tmp_submit_script:
                tmp_submit_script.write(str(script).encode("utf-8"))
                tmp_submit_script.flush()
                try:
                    subprocess.check_output(
                        submit_cmd + [tmp_submit_script.name], universal_newlines=True
                    )
                except subprocess.CalledProcessError as e:
                    raise SubmitError(f"sbatch error: {e.output}")

                return True

    @classmethod
    def is_present(cls):
        """Return True if a SLURM scheduler is detected."""
        try:
            subprocess.check_output(["sbatch", "--version"], stderr=subprocess.STDOUT)
        except OSError:
            return False
        else:
            return True
