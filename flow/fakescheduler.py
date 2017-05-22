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

    def submit(self, script, pretend=False, hold=False, flags=[], *args, **kwargs):
        def format_arg(k, v):
            if v is True:
                return '--{}'.format(k)
            elif v is False or v is None:
                return
            else:
                return '--{}={}'.format(k, v)

        fake_cmd = '--hold' if hold else ''
        if kwargs:
            fake_cmd += ' ' + ' '.join(filter(None, (format_arg(k, v) for k, v in kwargs.items())))
        if flags:
            fake_cmd += ' ' + ' '.join(flags)
        print("# Submit command: testsub {}".format(fake_cmd))
        print(script.read())
        # return status is None, meaning, 'not actually submitted'
        return None

    @classmethod
    def is_present(cls):
        return False
