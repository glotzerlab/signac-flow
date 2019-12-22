# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implementation of the scheduling system for a fake scheduler.

The FakeScheduler class can be used in place of a real scheduler to test
the cluster job submission workflow.
"""
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

    def submit(self, script, **kwargs):
        "Just print the script to screen."
        print(script)

    @classmethod
    def is_present(cls):
        return False
