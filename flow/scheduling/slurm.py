# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for SLURM schedulers.

This module implements the Scheduler class, and the JobCluster class
for SLURM schedulers.
"""
from __future__ import print_function
import getpass
import subprocess
import tempfile
import logging
import errno

from .base import Scheduler
from .base import ClusterJob, JobStatus


logger = logging.getLogger(__name__)


def _fetch(user=None):
    "Fetch the cluster job status information from the SLURM scheduler."

    def parse_status(s):
        s = s.strip()
        if s == 'PD':
            return JobStatus.queued
        elif s == 'R':
            return JobStatus.active
        elif s in ['CG', 'CD', 'CA', 'TO']:
            return JobStatus.inactive
        elif s in ['F', 'NF']:
            return JobStatus.error
        return JobStatus.registered

    if user is None:
        user = getpass.getuser()

    cmd = ['squeue', '-u', user, '-h', "--format=%2t%100j"]
    try:
        result = subprocess.check_output(cmd).decode('utf-8')
    except subprocess.CalledProcessError as error:
        print('error', error)
        raise
    except IOError as error:
        if error.errno != errno.ENOENT:
            raise
        else:
            raise RuntimeError("Slurm not available.")
    lines = result.split('\n')
    for line in lines:
        if line:
            status = line[:2]
            name = line[2:].rstrip()
            yield ClusterJob(name, parse_status(status))


class SlurmJob(ClusterJob):
    "A Slurm is a ClusterJob managed by a SLURM scheduler."
    pass


class SlurmScheduler(Scheduler):
    """Implementation of the abstract Scheduler class for TORQUE schedulers.

    This class allows us to submit cluster jobs to a TORQUE scheduler and query
    their current status.

    :param user:
        Limit the status information to cluster jobs submitted by user.
    :type user:
        str
    """
    # The standard command used to submit jobs to the SLURM scheduler.
    submit_cmd = ['sbatch']

    def __init__(self, user=None, **kwargs):
        super(SlurmScheduler, self).__init__(**kwargs)
        self.user = user

    def jobs(self):
        "Yield cluster jobs by querying the scheduler."
        self._prevent_dos()
        for job in _fetch(user=self.user):
            yield job

    def submit(self, script, after=None, hold=False, pretend=False, flags=None, **kwargs):
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
            Returns True if the cluster job was successfully submitted, otherwise None.
        """
        if flags is None:
            flags = []
        elif isinstance(flags, str):
            flags = flags.split()

        submit_cmd = self.submit_cmd + flags

        if after is not None:
            submit_cmd.extend(
                ['-W', 'depend="afterany:{}"'.format(after.split('.')[0])])

        if hold:
            submit_cmd += ['--hold']

        if pretend:
            print("# Submit command: {}".format('  '.join(submit_cmd)))
            print(script)
            print()
        else:
            with tempfile.NamedTemporaryFile() as tmp_submit_script:
                tmp_submit_script.write(str(script).encode('utf-8'))
                tmp_submit_script.flush()
                subprocess.check_output(submit_cmd + [tmp_submit_script.name])
                return True

    @classmethod
    def is_present(cls):
        "Return True if it appears that a SLURM scheduler is available within the environment."
        try:
            subprocess.check_output(['sbatch', '--version'], stderr=subprocess.STDOUT)
        except (IOError, OSError):
            return False
        else:
            return True
