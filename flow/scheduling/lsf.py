# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for LSF schedulers.

This module implements the Scheduler and ClusterJob classes for LSF.
"""
import getpass
import subprocess
import tempfile
import json
import logging
import errno

from .base import Scheduler
from .base import ClusterJob, JobStatus


logger = logging.getLogger(__name__)


def _parse_status(s):
    if s in ['PEND', 'WAIT']:
        return JobStatus.queued
    elif s == 'RUN':
        return JobStatus.active
    elif s in ['SSUSP', 'USUSP', 'PSUSP']:
        return JobStatus.held
    elif s == 'DONE':
        return JobStatus.inactive
    elif s == 'EXIT':
        return JobStatus.error
    return JobStatus.registered


def _fetch(user=None):
    "Fetch the cluster job status information from the LSF scheduler."

    if user is None:
        user = getpass.getuser()

    cmd = ['bjobs', '-json', '-u', user]
    try:
        result = json.loads(subprocess.check_output(cmd).decode('utf-8'))
    except subprocess.CalledProcessError:
        raise
    except IOError as error:
        if error.errno != errno.ENOENT:
            raise
        else:
            raise RuntimeError("LSF not available.")
    except json.decoder.JSONDecodeError:
        raise RuntimeError("Could not parse LSF JSON output.")

    for record in result['RECORDS']:
        yield LSFJob(record)


class LSFJob(ClusterJob):
    "An LSFJob is a ClusterJob managed by an LSF scheduler."

    def __init__(self, record):
        self.record = record
        self._job_id = record['JOBID']
        self._status = _parse_status(record['STAT'])

    def name(self):
        return self.record['JOB_NAME']


class LSFScheduler(Scheduler):
    """Implementation of the abstract Scheduler class for LSF schedulers.

    This class allows us to submit cluster jobs to a LSF scheduler and query
    their current status.

    :param user:
        Limit the status information to cluster jobs submitted by user.
    :type user:
        str
    """
    # The standard command used to submit jobs to the LSF scheduler.
    submit_cmd = ['bsub']

    def __init__(self, user=None, **kwargs):
        super(LSFScheduler, self).__init__(**kwargs)
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
            Please note: A successful "pretend" submission is not guaranteed to succeed.
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
                ['-w', '"done({})"'.format(after.split('.')[0])])

        if hold:
            submit_cmd += ['-H']

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
        "Return True if it appears that an LSF scheduler is available within the environment."
        try:
            subprocess.check_output(['bjobs', '-V'], stderr=subprocess.STDOUT)
        except (IOError, OSError):
            return False
        else:
            return True
