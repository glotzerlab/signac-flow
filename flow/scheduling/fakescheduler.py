# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for a fake scheduler.

The FakeScheduler class can be used in place of a real scheduler to test
the cluster job submission workflow.
"""
from __future__ import print_function
import logging

from .base import Scheduler

logger = logging.getLogger(__name__)


class FakeScheduler(Scheduler):
    """Implementation of the abstract Scheduler class for a fake scheduler.

    This scheduler does not actually schedule (or execute) any jobs, but it can be used
    to test the submission workflow.
    """

    def jobs(self):
        "Yields nothing, since the FakeScheduler does not actually schedule any jobs."
        return
        yield

    def submit(self, script, flags=None, *args, **kwargs):
        """Pretend to submit a script for execution to the scheduler.

        :param script:
            The job script submitted for execution.
        :type script:
            str
        :param flags:
            Additional arguments to pass through to the scheduler submission command.
        :type flags:
            list
        :returns:
            The cluster job id if the script was successfully submitted, otherwise None.
        """
        if flags is None:
            flags = []

        def format_arg(k, v):
            "Format a key-value pair for output such that it looks like a command line argument."
            if v is True:
                return '--{}'.format(k)
            elif v is False or v is None:
                return
            else:
                return '--{}={}'.format(k, v)

        fake_cmd = ""
        if kwargs:
            fake_cmd += ' '.join(filter(None, (format_arg(k, v) for k, v in kwargs.items()))) + ' '
        if flags:
            fake_cmd += ' '.join(flags) + ' '
        print("# Submit command: testsub {}".format(fake_cmd.strip()))
        print(script)
        # return status is None, meaning, 'not actually submitted'
        return None

    @classmethod
    def is_present(cls):
        return False
