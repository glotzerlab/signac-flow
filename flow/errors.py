# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Definitions of Exception classes used in this package."""


class SubmitError(RuntimeError):
    "Indicates an error during cluster job submission."
    pass


class NoSchedulerError(AttributeError):
    "Indicates that there is no scheduler type defined for an environment class."
    pass
