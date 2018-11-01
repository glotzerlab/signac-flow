# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import sys
import os
from expected_submit_outputs.project import TestProject
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
    project = TestProject.get_project(
        root=os.path.join(
            os.path.dirname(__file__),
            './expected_submit_outputs')
        )

    for job in project:
        with job:
            kwargs = job.statepoint()
            env = get_nested_attr(flow, kwargs['environment'])
            parameters = kwargs['parameters']
            if 'bundle' in parameters:
                bundle = parameters.pop('bundle')
                fn = 'script_{}.sh'.format('_'.join(bundle))
                with open(fn, 'w') as f:
                    sys.stdout = f
                    project.submit(env=env, jobs=[job], names=bundle, pretend=True, force=True, bundle_size=len(bundle), **parameters)
            else:
                for op in project.operations:
                    if 'partition' in parameters:
                        # Don't try to submit GPU operations to CPU partitions
                        # and vice versa.  We should be able to relax this
                        # requirement if we make our error checking more
                        # consistent.
                        if (('gpu' not in parameters['partition'].lower() and
                             'gpu' in op.lower()) or
                            ('gpu' in parameters['partition'].lower() and
                             'gpu' not in op.lower())):
                                continue
                    fn = 'script_{}.sh'.format(op)
                    with open(fn, 'w') as f:
                        sys.stdout = f
                        project.submit(env=env, jobs=[job], names=[op], pretend=True, force=True, **parameters)
