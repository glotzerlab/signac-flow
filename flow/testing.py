# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Initialize a project for testing purposes

"""
import signac
from .template import init


def make_project(alias='project', root=None, **kwargs):
    """Initialize a project for testing purposes

    The initialized project has a few operations and a few jobs that are in
    various points in the workflow defined by the project.

    """
    init(alias=alias, root=root, template='testing')
    project = signac.init_project(name=alias, root=root)
    init_jobs(project, **kwargs)


def init_jobs(project, nested=False, listed=False, heterogeneous=False):
    """Initialize a dataspace for testing purposes

    """
    vals = [1, 1.0, '1', True, None]
    if nested:
        vals += [{'b': v, 'c': 0} if heterogeneous else {'b': v} for v in vals]
    if listed:
        vals += [[v, 0] if heterogeneous else [v] for v in vals]
    for v in vals:
        project.open_job(dict(a=v)).init()
