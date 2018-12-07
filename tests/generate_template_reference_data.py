# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function

import sys
import os
import re
import io
import operator
import itertools
import argparse
from hashlib import sha1

import signac
import flow
import flow.environments

from test_project import redirect_stdout

# Define a consistent submission name so that we can test that job names are
# being correctly generated.
PROJECT_NAME = "SubmissionTest"
ARCHIVE_DIR = os.path.join(
    os.path.dirname(__file__), './template_reference_data.tar.gz')
PROJECT_DIRECTORY = "/home/johndoe/project/"


def cartesian(**kwargs):
    """Generate a set of statepoint dictionaries from a dictionary of the form
    {key1: [list of values], key2: [list of values]...}"""
    for combo in itertools.product(*kwargs.values()):
        yield dict(zip(kwargs.keys(), combo))


def get_nested_attr(obj, attr, default=None):
    """Get nested attributes of an object."""
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


def in_line(patterns, line):
    """Check if any of the strings in the list patterns are in the line"""
    return any([p in line for p in patterns])


def init(project):
    """Initialize the data space for the given project."""
    # This object is a dictionary whose keys are environments. Each environment
    # is associated with a list of dictionaries, where each dictionary contains
    # a set of parameters that need to be tested together. For instance
    # bundling and parallelism must exist in the same test. The goal is to
    # construct a minimal covering set of all test cases.
    environments = {
            'environment.UnknownEnvironment': [],
            'environments.xsede.CometEnvironment': [
                {
                    'partition': ['compute', 'shared', 'gpu'],
                    'walltime': [None, 1],
                },
                {
                    'partition': ['compute'],
                    'nn': [None, 1, 2],
                },
                {
                    'partition': ['compute'],
                    'parallel': [False, True],
                    'bundle': [['mpi_op', 'omp_op']],
                }
            ],
            'environments.xsede.Stampede2Environment': [
                {
                    'partition': ['skx-normal'],
                    'walltime': [None, 1],
                },
                {
                    'partition': ['skx-normal'],
                    'nn': [None, 1, 2],
                },
                {
                    'partition': ['skx-normal'],
                    'parallel': [False, True],
                    'bundle': [['mpi_op', 'omp_op']]
                }
            ],
            'environments.xsede.BridgesEnvironment': [
                {
                    'partition': ['RM', 'RM-Shared', 'GPU'],
                    'walltime': [None, 1],
                },
                {
                    'partition': ['RM'],
                    'nn': [None, 1, 2],
                },
                {
                    'partition': ['RM'],
                    'parallel': [False, True],
                    'bundle': [['mpi_op', 'omp_op']],
                }
            ],
            'environments.umich.FluxEnvironment': [
                {
                    'walltime': [None, 1],
                },
                {
                    'nn': [None, 1, 2],
                },
                {
                    'parallel': [False, True],
                    'bundle': [['mpi_op', 'omp_op']],
                }
            ],
            'environments.incite.TitanEnvironment': [
                {
                    'walltime': [None, 1],
                },
                {
                    'nn': [None, 1, 2],
                },
                {
                    'parallel': [False, True],
                    'bundle': [['mpi_op', 'omp_op']],
                }
            ],
            'environments.incite.EosEnvironment': [
                {
                    'walltime': [None, 1],
                },
                {
                    'nn': [None, 1, 2],
                },
                {
                    'parallel': [False, True],
                    'bundle': [['mpi_op', 'omp_op']],
                }
            ]
        }

    for environment, parameter_combinations in environments.items():
        for parameter_sets in parameter_combinations:
            params = cartesian(**parameter_sets)
            for param in params:
                sp = dict(environment=environment, parameters=param)
                project.open_job(sp).init()


# Mock the bundle storing to avoid needing to make a file
def _store_bundled(self, operations):
    if len(operations) == 1:
        return operations[0].get_id()
    else:
        h = '.'.join(op.get_id() for op in operations)
        bid = '{}/bundle/{}'.format(self, sha1(h.encode('utf-8')).hexdigest())
        return bid

flow.FlowProject._store_bundled = _store_bundled


class TestProject(flow.FlowProject):
    N = 2


@TestProject.operation
def serial_op(job):
    pass


@TestProject.operation
@flow.directives(np=TestProject.N)
def parallel_op(job):
    pass


@TestProject.operation
@flow.directives(nranks=TestProject.N)
def mpi_op(job):
    pass


@TestProject.operation
@flow.directives(omp_num_threads=TestProject.N)
def omp_op(job):
    pass


@TestProject.operation
@flow.directives(nranks=TestProject.N, omp_num_threads=TestProject.N)
def hybrid_op(job):
    pass


@TestProject.operation
@flow.directives(ngpu=TestProject.N)
def gpu_op(job):
    pass


@TestProject.operation
@flow.directives(ngpu=TestProject.N, nranks=TestProject.N)
def mpi_gpu_op(job):
    pass


def get_masked_flowproject(p):
    """Mock environment-dependent attributes and functions. Need to mock
    sys.executable before the FlowProject is instantiated, and then modify the
    root_directory and project_dir elements after creation."""
    sys.executable = '/usr/local/bin/python'
    for _, op in TestProject._OPERATION_FUNCTIONS:
        op._flow_path = 'generate_template_reference_data.py'
    fp = TestProject.get_project(root=p.root_directory())
    fp.root_directory = lambda: PROJECT_DIRECTORY
    fp.config.project_dir = PROJECT_DIRECTORY
    return fp

def main(args):
    # If the ARCHIVE_DIR already exists, only recreate if forced.
    if os.path.exists(ARCHIVE_DIR):
        if args.force:
            os.unlink(ARCHIVE_DIR)
        else:
            return

    # NOTE: We should replace the below line with 
    # with signac.TemporaryProject(name=PROJECT_NAME, cls=TestProject) as fp:
    # once the next version of signac is released, and we can then remove
    # the additional FlowProject instantiation below
    with signac.TemporaryProject(name=PROJECT_NAME) as p:
        init(p)
        fp = get_masked_flowproject(p)

        for job in fp:
            with job:
                kwargs = job.statepoint()
                env = get_nested_attr(flow, kwargs['environment'])
                parameters = kwargs['parameters']
                if 'bundle' in parameters:
                    bundle = parameters.pop('bundle')
                    fn = 'script_{}.sh'.format('_'.join(bundle))
                    tmp_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
                    with redirect_stdout(tmp_out):
                        fp.submit(
                            env=env, jobs=[job], names=bundle, pretend=True,
                            force=True, bundle_size=len(bundle), **parameters)

                    # Filter out non-header lines
                    tmp_out.seek(0)
                    with open(fn, 'w') as f:
                        with redirect_stdout(f):
                            print(tmp_out.read(), end='')
                else:
                    for op in fp.operations:
                        if 'partition' in parameters:
                            # Don't try to submit GPU operations to CPU partitions
                            # and vice versa.  We should be able to relax this
                            # requirement if we make our error checking more
                            # consistent.
                            if operator.xor('gpu' in parameters['partition'].lower(),
                                            'gpu' in op.lower()):
                                    continue
                        fn = 'script_{}.sh'.format(op)
                        tmp_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
                        with redirect_stdout(tmp_out):
                            fp.submit(
                                env=env, jobs=[job], names=[op], pretend=True,
                                force=True, **parameters)

                        # Filter out non-header lines and the job-name line
                        tmp_out.seek(0)
                        with open(fn, 'w') as f:
                            with redirect_stdout(f):
                                print(tmp_out.read(), end='')

        # For compactness, we move the output into an ARCHIVE_DIR then delete the original data.
        fp.export_to(
            target=ARCHIVE_DIR)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate reference submission scripts for various environments")
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help="Recreate the data space even if the ARCHIVE_DIR already exists"
    )
    main(parser.parse_args())
