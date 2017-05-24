# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""The environments module contains additional *opt-in* environment profiles.

Add the following line to your project modules, to use these profiles:

.. code-block:: python

    import flow.environments

"""
from . import incite
from . import xsede
from . import umich

__all__ = [
    'incite',
    'xsede',
    'umich',
    ]
