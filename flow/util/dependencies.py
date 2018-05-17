# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Functions to test the import of soft-dependencies."""


def _requires_jinja2(function_name='This signac-flow function'):
    "Check if jinja2 is available, otherwise raise RuntimeError with descriptive message."
    try:
        import jinja2  # noqa
    except ImportError:
        raise RuntimeError("Error: {} requires the 'jinja2' package.".format(function_name))
