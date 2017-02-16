# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define environments to automate submission scripts."""
from __future__ import print_function
import re
import socket
import logging
import io
from collections import OrderedDict


from signac.common.six import with_metaclass
from . import scheduler
from . import manage


logger = logging.getLogger(__name__)


def format_timedelta(delta):
    hours, r = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(r, 60)
    hours += delta.days * 24
    return "{:0>2}:{:0>2}:{:0>2}".format(hours, minutes, seconds)


class ComputeEnvironmentType(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = OrderedDict()
        else:
            cls.registry[name] = cls
        return super(ComputeEnvironmentType, cls).__init__(name, bases, dct)


class JobScript(io.StringIO):
    "Simple StringIO wrapper to implement cmd wrapping logic."
    eol = '\n'

    def __init__(self, parent):
        self._parent = parent
        super().__init__()

    def writeline(self, line=''):
        "Write one line to the job script."
        self.write(line + self.eol)

    def write_cmd(self, cmd, np=1, bg=False):
        """Write a command to the jobscript.

        This command wrapper function is a convenience function, which
        adds mpi and other directives whenever necessary.

        :param cmd: The command to write to the jobscript.
        :type cmd: str
        :param np: The number of processors required for execution.
        :type np: int
        """
        if np > 1:
            cmd = self._parent.mpi_cmd(cmd, np=np)
        if bg:
            cmd = self._parent.bg(cmd)
        self.writeline(cmd)


class ComputeEnvironment(with_metaclass(ComputeEnvironmentType)):
    scheduler = None
    hostname_pattern = None

    @classmethod
    def script(cls, **kwargs):
        return JobScript(cls)

    @classmethod
    def is_present(cls):
        if cls.hostname_pattern is None:
            return False
        else:
            return re.match(
                cls.hostname_pattern, socket.gethostname()) is not None

    @classmethod
    def get_scheduler(cls):
        try:
            return getattr(cls, 'scheduler_type')()
        except AttributeError:
            raise AttributeError("You must define a scheduler type for every environment")

    @classmethod
    def submit(cls, script, *args, **kwargs):
        # Hand off the actual submission to the scheduler
        script.seek(0)
        if cls.get_scheduler().submit(script, *args, **kwargs):
            return manage.JobStatus.submitted

    @staticmethod
    def bg(cmd):
        return cmd + ' &'


class UnknownEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.FakeScheduler

    @classmethod
    def script(cls, **kwargs):
        js = super(UnknownEnvironment, cls).script(**kwargs)
        for key in sorted(kwargs):
            js.writeline('#TEST {}={}'.format(key, kwargs[key]))
        return js


class TestEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.FakeScheduler


class MoabEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.MoabScheduler


class SlurmEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.SlurmScheduler


class CPUEnvironment(ComputeEnvironment):
    pass


class GPUEnvironment(ComputeEnvironment):
    pass


def get_environment(test=False):
    if test:
        return TestEnvironment
    else:
        for env_type in reversed(ComputeEnvironment.registry.values()):
            if env_type.is_present():
                return env_type
        else:
            return UnknownEnvironment
