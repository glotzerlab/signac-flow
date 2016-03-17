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
