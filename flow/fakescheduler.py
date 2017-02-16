# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import logging

from .manage import Scheduler, JobStatus

logger = logging.getLogger(__name__)


class FakeScheduler(Scheduler):

    def jobs(self):
        return
        yield

    def submit(self, script, *args, **kwargs):
        fake_cmd = ' '.join('--{}{}'.format(k, '' if v is True else v) for k, v in kwargs.items())
        print("# Submit command: testsub {}".format(fake_cmd))
        print(script.read())
