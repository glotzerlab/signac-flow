# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for the built-in simple scheduler."""
import json
import os
import subprocess
import tempfile

from .base import ClusterJob, JobStatus, Scheduler


class SimpleScheduler(Scheduler):
    """Implementation of the abstract Scheduler class for SimpleScheduler.

    This class allows us to submit cluster jobs to the built-in simple
    scheduler and query their current status.
    """

    @classmethod
    def is_present(cls):
        return bool(os.environ.get("SIMPLE_SCHEDULER"))

    def __init__(self):
        self.cmd = os.environ["SIMPLE_SCHEDULER"].split()

    def jobs(self):
        cmd = self.cmd + ["status", "--json"]
        status = json.loads(subprocess.check_output(cmd).decode("utf-8"))
        for _id, doc in status.items():
            yield ClusterJob(doc["job_name"], JobStatus(doc["status"]))

    def submit(self, script, pretend=False, **kwargs):
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
