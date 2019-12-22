# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""The environments module contains a set of provided environment profiles.

These environments are imported by default. This can be disabled by setting
the configuration key 'flow.import_packaged_environments' to 'off' with the
following shell command:

.. code-block:: bash

    signac config --global set flow.import_packaged_environments off

"""
from . import incite
from . import xsede
from . import umich

__all__ = [
    'incite',
    'xsede',
    'umich',
    ]
