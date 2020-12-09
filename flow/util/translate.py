# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Helper functions to keep track of abbreviations meant for output on screen."""


def abbreviate(x, a):
    """Abbreviate x with a and add to the abbreviation table."""
    if x == a:
        return x
    else:
        abbreviate.table[a] = x
        return a


abbreviate.table = {}  # noqa


def shorten(x, max_length=None):
    """Shorten x to max_length and add to abbreviation table."""
    if max_length is None:
        return x
    else:
        return abbreviate(x, x[:max_length])
