# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import subprocess
from signac.common import six

if six.PY2:
    # Timeouts are not support for Python 2.7. The class defined below is
    # a pure mock class.
    class TimeoutExpired(RuntimeError):
        pass
else:
    from subprocess import TimeoutExpired


def fork(cmd, timeout=None):
    "Helper function for py2/3 compatible execution of forked processes."
    if timeout is not None:
        raise RuntimeError("Executing with a timeout is not supported in Python 2.7.")

    if six.PY2:
        subprocess.call(cmd, shell=True)
    else:
        subprocess.call(cmd, shell=True, timeout=timeout)


__all__ = ['fork', 'TimeoutExpired']
