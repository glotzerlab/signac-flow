# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Routines for the SLURM scheduler environment."""
from __future__ import print_function
import getpass
import subprocess
import tempfile
import logging

from .manage import Scheduler
from .manage import ClusterJob, JobStatus


logger = logging.getLogger(__name__)


def _fetch(user=None):
    def parse_status(s):
        if s == 'Q':
            return JobStatus.queued
        elif s == 'R':
            return JobStatus.active
        elif s == 'C':
            return JobStatus.inactive
        else:
            assert 0

    if user is None:
        user = getpass.getuser()
    cmd = "qstat -f"
    try:
        result = subprocess.check_output(cmd.split()).decode()
    except subprocess.CalledProcessError as error:
        if error.returncode == 153:
            return
        else:
            raise
    except FileNotFoundError:
        raise RuntimeError("Slurm not available.")
    jobs = result.split('\n\n')
    for jobinfo in jobs:
        if not jobinfo:
            continue
        lines = jobinfo.split('\n')
        assert lines[0].startswith('Job Id:')
        info = dict()
        for line in lines[1:]:
            tokens = line.split('=')
            info[tokens[0].strip()] = tokens[1].strip()
        if info['euser'].startswith(user):
            yield SlurmJob(info['Job_Name'], parse_status(info['job_state']))


class SlurmJob(ClusterJob):
    pass


class SlurmScheduler(Scheduler):
    submit_cmd = ['sbatch']

    def __init__(self, user=None, **kwargs):
        super(SlurmScheduler, self).__init__(**kwargs)
        self.user = user

    def jobs(self):
        self._prevent_dos()
        for job in _fetch(user=self.user):
            yield job

    def submit(self, script, after=None, hold=False, pretend=False, flags=None, **kwargs):
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
            print(script.read())
            print()
        else:
            with tempfile.NamedTemporaryFile() as tmp_submit_script:
                tmp_submit_script.write(script.read().encode('utf-8'))
                tmp_submit_script.flush()
                subprocess.check_output(submit_cmd + [tmp_submit_script.name])
                return True

    @classmethod
    def is_present(cls):
        try:
            subprocess.check_output(['sbatch', '--version'], stderr=subprocess.STDOUT)
        except (IOError, OSError):
            return False
        else:
            return True
