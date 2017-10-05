# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Routines for the SLURM scheduler environment."""
from __future__ import print_function
import getpass
import subprocess
import tempfile
import logging
import errno

from .manage import Scheduler
from .manage import ClusterJob, JobStatus


logger = logging.getLogger(__name__)


def _fetch(user=None):

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

    cmd = ['squeue', '-u', user, '-h', '-o "%2t %100j"']
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
            status, name = line.strip()[1:-1].split()
            yield ClusterJob(name, parse_status(status))


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
