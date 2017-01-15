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
import xml.etree.ElementTree as ET

from .manage import Scheduler
from .manage import ClusterJob, JobStatus


logger = logging.getLogger(__name__)


def _fetch(user=None):
    if user is None:
        user = getpass.getuser()
    cmd = "qstat -fx -u {user}".format(user=user)
    try:
        result = io.BytesIO(subprocess.check_output(cmd.split()))
    except FileNotFoundError:
        raise RuntimeError("Moab not available.")
    tree = ET.parse(source=result)
    return tree.getroot()


def format_timedelta(delta):
    hours, r = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(r, 60)
    hours += delta.days * 24
    return "{:0>2}:{:0>2}:{:0>2}".format(hours, minutes, seconds)


class MoabJob(ClusterJob):

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


class MoabScheduler(Scheduler):
    submit_cmd = ['qsub']

    def __init__(self, root=None, user=None, header=None, cores_per_node=None):
        self.user = user
        self.root = root
        self.header = header
        self.cores_per_node = cores_per_node

    def jobs(self):
        self._prevent_dos()
        nodes = _fetch(user=self.user)
        for node in nodes.findall('Job'):
            yield MoabJob(node)

    def submit(self, jobsid, np, walltime, script, resume=None,
               after=None, pretend=False, hold=False, *args, **kwargs):
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
            np=np, nn=num_nodes,
            walltime=format_timedelta(walltime), jobsid=jobsid)
        if pretend:
            print("#\n# Pretend to submit:\n")
            print(submit, "\n")
        else:
            submit_cmd = self.submit_cmd
            if after is not None:
                submit_cmd.extend(
                    ['-W', 'depend="afterok:{}"'.format(after.split('.')[0])])
            if hold:
                submit_cmd += ['-h']
            with tempfile.NamedTemporaryFile() as tmp_submit_script:
                tmp_submit_script.write(submit.encode('utf-8'))
                tmp_submit_script.flush()
                output = subprocess.check_output(
                    submit_cmd + [tmp_submit_script.name])
            jobsid = output.decode('utf-8').strip()
            return jobsid
