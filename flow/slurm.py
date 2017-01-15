# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Routines for the MOAB environment."""

from __future__ import print_function
import io
import getpass
import subprocess
import tempfile
import math
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


def format_timedelta(delta):
    hours, r = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(r, 60)
    hours += delta.days * 24
    return "{:0>2}:{:0>2}:{:0>2}".format(hours, minutes, seconds)


class SlurmJob(ClusterJob):
    pass


class SlurmScheduler(Scheduler):
    submit_cmd = ['sbatch']

    def __init__(self, user=None, header=None, cores_per_node=None):
        self.header = header
        self.cores_per_node = cores_per_node
        self.user = user

    def jobs(self):
        self._prevent_dos()
        for job in _fetch(user=self.user):
            yield job

    def submit(self, jobsid, np, walltime, script, resume=None,
               after=None, pretend=False, *args, **kwargs):
        submit_script = io.StringIO()
        num_nodes = math.ceil(np / self.cores_per_node)
        if (np / (num_nodes * self.cores_per_node)) < 0.9:
            logger.warning("Bad node utilization!")
        submit_script.write(self.header.format(
            jobsid=jobsid, nn=num_nodes, walltime=format_timedelta(walltime)))
        submit_script.write('\n')
        submit_script.write(script.read())
        submit_script.seek(0)
        submit = submit_script.read().format(
            np=np, nn=num_nodes, walltime=format_timedelta(walltime), jobsid=jobsid)
        if pretend:
            print("#\n# Pretend to submit:\n")
            print(submit, "\n")
        else:
            submit_cmd = self.submit_cmd
            if after is not None:
                submit_cmd.extend(
                    ['-W', 'depend="afterany:{}"'.format(after.split('.')[0])])
            with tempfile.NamedTemporaryFile() as tmp_submit_script:
                tmp_submit_script.write(submit.encode('utf-8'))
                tmp_submit_script.flush()
                output = subprocess.check_output(
                    submit_cmd + [tmp_submit_script.name])
            return jobsid
