# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for TORQUE schedulers.

This module implements the Scheduler class, and the JobCluster class
for TORQUE schedulers.
"""
from __future__ import print_function
import io
import getpass
import subprocess
import tempfile
import logging
import xml.etree.ElementTree as ET

from .base import Scheduler
from .base import ClusterJob, JobStatus


logger = logging.getLogger(__name__)


def _fetch(user=None):
    "Fetch the cluster job status information from the TORQUE scheduler."
    if user is None:
        user = getpass.getuser()
    cmd = "qstat -fx -u {user}".format(user=user)
    try:
        result = io.BytesIO(subprocess.check_output(cmd.split()))
    except FileNotFoundError:
        raise RuntimeError("Torque not available.")
    tree = ET.parse(source=result)
    return tree.getroot()


class TorqueJob(ClusterJob):
    "Implementation of the abstract ClusterJob class for TORQUE schedulers."

    def __init__(self, node):
        self.node = node

    def _id(self):
        return self.node.find('Job_Id').text

    def __str__(self):
        return str(self._id())

    def name(self):
        return self.node.find('Job_Name').text

    def status(self):
        job_state = self.node.find('job_state').text
        if job_state == 'R':
            return JobStatus.active
        if job_state == 'Q':
            return JobStatus.queued
        if job_state == 'C':
            return JobStatus.inactive
        if job_state == 'H':
            return JobStatus.held
        return JobStatus.registered


class TorqueScheduler(Scheduler):
    """Implementation of the abstract Scheduler class for TORQUE schedulers.

    This class allows us to submit cluster jobs to a TORQUE scheduler and query
    their current status.

    :param user:
        Limit the status information to cluster jobs submitted by user.
    :type user:
        str
    """
    # The standard command used to submit jobs to the TORQUE scheduler.
    submit_cmd = ['qsub']

    def __init__(self, user=None, **kwargs):
        super(TorqueScheduler, self).__init__(**kwargs)
        self.user = user

    def jobs(self):
        "Yield cluster jobs by querying the scheduler."
        self._prevent_dos()
        nodes = _fetch(user=self.user)
        for node in nodes.findall('Job'):
            yield TorqueJob(node)

    def submit(self, script, after=None, pretend=False, hold=False, flags=None, *args, **kwargs):
        """Submit a job script for execution to the scheduler.

        :param script:
            The job script submitted for execution.
        :type script:
            str
        :param after:
            Execute the submitted script after a job with this id has completed.
        :type after:
            str
        :param pretend:
            If True, do not actually submit the script, but only simulate the submission.
            Can be used to test whether the submission would be successful.
            Please note: A successful "pretend" submission is not guaranteed to succeeed.
        :type pretend:
            bool
        :param flags:
            Additional arguments to pass through to the scheduler submission command.
        :type flags:
            list
        :returns:
            The cluster job id if the script was successfully submitted, otherwise None.
        """
        if flags is None:
            flags = []
        elif isinstance(flags, str):
            flags = flags.split()

        submit_cmd = self.submit_cmd + flags

        if after is not None:
            submit_cmd.extend(
                ['-W', 'depend="afterok:{}"'.format(after.split('.')[0])])

        if hold:
            submit_cmd += ['-h']

        if pretend:
            print("# Submit command: {}".format(' '.join(submit_cmd)))
            print(script)
            print()
        else:
            with tempfile.NamedTemporaryFile() as tmp_submit_script:
                tmp_submit_script.write(str(script).encode('utf-8'))
                tmp_submit_script.flush()
                output = subprocess.check_output(
                    submit_cmd + [tmp_submit_script.name])
            jobsid = output.decode('utf-8').strip()
            return jobsid

    @classmethod
    def is_present(cls):
        "Return True if it appears that a TORQUE scheduler is available within the environment."
        try:
            subprocess.check_output(['qsub', '--version'], stderr=subprocess.STDOUT)
        except (IOError, OSError):
            return False
        else:
            return True
