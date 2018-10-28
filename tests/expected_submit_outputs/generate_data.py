# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import sys
from .project import TestProject
import flow.environments
from flow import FlowProject


def get_nested_attr(obj, attr, default=None):
    """Get nested attributes"""
    attrs = attr.split('.')
    for a in attrs:
        try:
            obj = getattr(obj, a)
        except AttributeError:
            if default:
                return default
            else:
                raise
    return obj

if __name__ == "__main__":
    project = TestProject()

    for job in project:
        with job:
            for op in project.operations:
                fn = 'script_{}.sh'.format(op)
                with open(fn, 'w') as f:
                    sys.stdout = f
                    kwargs = job.statepoint()
                    env = get_nested_attr(flow, kwargs.pop('environment'))
                    project.submit(env=env, jobs=[job], names=[op], pretend=True, force=True, **kwargs)
