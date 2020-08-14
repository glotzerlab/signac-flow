# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest
import sys
import os
import io
import operator

import signac
import flow
import flow.environments

import generate_template_reference_data as gen
from test_project import redirect_stdout, redirect_stderr


def _env_name(env):
    name = '{}.{}'.format(env.__module__, env.__name__)
    return '.'.join(name.split('.')[1:])


def find_envs():
    """Yields the environments to be tested."""
    for name, env in flow.environment.ComputeEnvironment.registry.items():
        if env.__module__.startswith('flow.environments'):
            yield env


def mock_submit(fp, env, jobs, names, bundle_size, **kwargs):
    tmp_out = io.TextIOWrapper(
        io.BytesIO(), sys.stdout.encoding)
    with open(os.devnull, 'w') as devnull:
        with redirect_stderr(devnull):
            with redirect_stdout(tmp_out):
                fp.submit(
                    env=env, bundle_size=bundle_size, jobs=jobs, names=names,
                    force=True, pretend=True, **kwargs)
    tmp_out.seek(0)
    return tmp_out


@pytest.mark.parametrize('env', find_envs())
def test_env(env):
    # Force asserts to show the full file when failures occur.
    # Useful to debug errors that arise.

    # Must import the data into the project.
    with signac.TemporaryProject(name=gen.PROJECT_NAME) as p:
        fp = gen.get_masked_flowproject(p)
        fp.import_from(origin=gen.ARCHIVE_DIR)
        jobs = fp.find_jobs(dict(environment=_env_name(env)))
        if not len(jobs):
            raise RuntimeError(
                "No reference data for environment {}!".format(_env_name(env))
                )
        reference = []
        generated = []
        agg_ops = ['serial_agg_op', 'parallel_agg_op', 'group2']
        for job in jobs:
            parameters = job.sp.parameters()
            if 'bundle' in parameters:
                bundle = parameters.pop('bundle')
                tmp_out = mock_submit(fp, env, [job], bundle, len(bundle), **parameters)
                msg = f"---------- Bundled submission of job {job}"
                generated.extend([msg] + tmp_out.read().splitlines())

                with open(job.fn('script_{}.sh'.format('_'.join(bundle)))) as file:
                    reference.extend([msg] + file.read().splitlines())
            else:
                for op in {**fp.operations, **fp.groups}:
                    if 'partition' in parameters:
                        # Don't try to submit GPU operations to CPU partitions
                        # and vice versa.  We should be able to relax this
                        # requirement if we make our error checking more
                        # consistent.
                        if operator.xor(
                            'gpu' in parameters['partition'].lower(),
                                'gpu' in op.lower()):
                            continue
                    if op in agg_ops:
                        continue
                        # tmp_out = mock_submit(fp, env, None, [op], 1, **parameters)
                        # msg = f"---------- Submission of operation {op} for jobs " \
                        #       f"{' '.join(map(str, jobs))}."
                    else:
                        tmp_out = mock_submit(fp, env, [job], [op], 1, **parameters)
                        msg = f"---------- Submission of operation {op} for job {job}."
                    generated.extend([msg] + tmp_out.read().splitlines())

                    with open(job.fn('script_{}.sh'.format(op))) as file:
                        reference.extend([msg] + file.read().splitlines())
        assert '\n'.join(reference) == '\n'.join(generated)
