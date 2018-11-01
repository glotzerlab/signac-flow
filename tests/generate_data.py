# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import sys
import os
from expected_submit_outputs.project import TestProject
import flow.environments
from flow import FlowProject
from contextlib import contextmanager
from io import TextIOWrapper, BytesIO
import re


@contextmanager
def redirect_stdout(new_target):
    "Temporarily redirect all output to stdout to new_target."
    old_target = sys.stdout
    try:
        sys.stdout = new_target
        yield
    finally:
        sys.stdout = old_target


@contextmanager
def redirect_stderr(new_target):
    "Temporarily redirect all output to stderr to new_target."
    old_target = sys.stderr
    try:
        sys.stderr = new_target
        yield
    finally:
        sys.stderr = old_target


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

    # This regex will be used to filter out the final hash in the job name.
    name_regex = r'(.*)\/[a-z0-9]*'

    for job in project:
        with job:
            kwargs = job.statepoint()
            env = get_nested_attr(flow, kwargs['environment'])
            parameters = kwargs['parameters']
            if 'bundle' in parameters:
                bundle = parameters.pop('bundle')
                fn = 'script_{}.sh'.format('_'.join(bundle))
                tmp_out = TextIOWrapper(BytesIO(), sys.stdout.encoding)
                with redirect_stdout(tmp_out):
                    project.submit(env=env, jobs=[job], names=bundle, pretend=True, force=True, bundle_size=len(bundle), **parameters)

                # Filter out non-header lines
                tmp_out.seek(0)
                with open(fn, 'w') as f:
                    with redirect_stdout(f):
                        for line in tmp_out:
                            if '#PBS' in line or '#SBATCH' in line or 'OMP_NUM_THREADS' in line:
                                if '#PBS -N' in line or '#SBATCH --job-name' in line:
                                    match = re.match(name_regex, line)
                                    print(match.group(1) + '\n', end='')
                                else:
                                    print(line, end='')
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
                    tmp_out = TextIOWrapper(BytesIO(), sys.stdout.encoding)
                    with redirect_stdout(tmp_out):
                        project.submit(env=env, jobs=[job], names=[op], pretend=True, force=True, **parameters)

                    # Filter out non-header lines and the job-name line
                    tmp_out.seek(0)
                    with open(fn, 'w') as f:
                        with redirect_stdout(f):
                            for line in tmp_out:
                                if '#PBS' in line or '#SBATCH' in line or 'OMP_NUM_THREADS' in line:
                                    if '#PBS -N' in line or '#SBATCH --job-name' in line:
                                        match = re.match(name_regex, line)
                                        print(match.group(1) + '\n', end='')
                                    else:
                                        print(line, end='')
