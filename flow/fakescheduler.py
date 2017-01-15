# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import logging

from .manage import Scheduler

logger = logging.getLogger(__name__)


class FakeScheduler(Scheduler):

    def jobs(self):
        return
        yield

    def submit(self, jobsid, np, walltime, script, *args, **kwargs):
        logger.info("Fake scheduling of job '{}' on {} procs with "
                    "a walltime of '{}'.".format(jobsid, np, walltime))
        for line in script:
            print(line)
        return jobsid
