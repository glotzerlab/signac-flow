# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for the bundled ``simple-scheduler`` script."""
import json
import os
import subprocess
import tempfile

from .base import ClusterJob, JobStatus, Scheduler


class SimpleScheduler(Scheduler):
    """Implementation of the abstract Scheduler class for the bundled ``simple-scheduler``.

    The package signac-flow includes a script in ``bin/simple-scheduler`` that
    is a simple model of a cluster job scheduler. The ``simple-scheduler``
    script is designed primarily for testing and demonstration.

    This class can submit cluster jobs to the built-in simple scheduler and
    query their current status.
    """

    @classmethod
    def is_present(cls):
        """Return True if a SimpleScheduler is detected."""
        return bool(os.environ.get("SIMPLE_SCHEDULER"))

    def __init__(self):
        self.cmd = os.environ["SIMPLE_SCHEDULER"].split()

    def jobs(self):
        """Yield cluster jobs by querying the scheduler."""
        cmd = self.cmd + ["status", "--json"]
        status = json.loads(subprocess.check_output(cmd).decode("utf-8"))
        for _id, doc in status.items():
            yield ClusterJob(doc["job_name"], JobStatus(doc["status"]))

    def submit(self, script, pretend=False, **kwargs):
        r"""Submit a job script for execution to the scheduler.

        Parameters
        ----------
        script : str
            The job script submitted for execution.
        pretend : bool
            If True, do not actually submit the script, but only simulate the
            submission. Can be used to test whether the submission would be
            successful. A successful "pretend" submission is not
            guaranteed to succeed. (Default value = False)
        \*\*kwargs
            Keyword arguments (ignored).

        Returns
        -------
        bool
            Returns True if the cluster job was successfully submitted,
            otherwise None.

        """
        cmd = self.cmd + ["submit"]
        if pretend:
            print("# Submit command: {}".format(" ".join(cmd)))
            print(script)
            print()
        else:
            with tempfile.NamedTemporaryFile() as tmp_submit_script:
                tmp_submit_script.write(str(script).encode("utf-8"))
                tmp_submit_script.flush()
                subprocess.check_output(cmd + [tmp_submit_script.name])
                return True
