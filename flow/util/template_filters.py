# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Provide jinja2 template environment filter functions."""


def _identical(iterable):
    """Check that all elements of an iterator are identical"""
    return len(set(iterable)) <= 1


def _format_timedelta(delta):
    "Format a time delta for interpretation by schedulers."
    if isinstance(delta, int) or isinstance(delta, float):
        import datetime
        delta = datetime.timedelta(hours=delta)
    hours, r = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(r, 60)
    hours += delta.days * 24
    return "{:0>2}:{:0>2}:{:0>2}".format(hours, minutes, seconds)


def _with_np_offset(operations):
    """Add the np_offset variable to the operations' directives."""
    offset = 0
    for operation in operations:
        operation.directives.setdefault('np_offset', offset)
        offset += operation.directives['np']
    return operations
