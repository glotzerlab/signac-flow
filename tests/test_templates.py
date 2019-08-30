# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import unittest

import sys
import os
import io
import operator

import signac
import flow
import flow.environments

import generate_template_reference_data as gen
from test_project import redirect_stdout, redirect_stderr


class BaseTemplateTest(object):
    project_class = signac.Project
    env = None

    def env_name(self):
        name = '{}.{}'.format(self.env.__module__, self.env.__name__)
        return '.'.join(name.split('.')[1:])

    def test_get_TestEnvironment(self):
        # Force asserts to show the full file when failures occur.
        # Useful to debug errors that arise.
        self.maxDiff = None

        # Must import the data into the project.
        with signac.TemporaryProject(name=gen.PROJECT_NAME) as p:
            fp = gen.get_masked_flowproject(p)
            fp.import_from(origin=gen.ARCHIVE_DIR)

            jobs = fp.find_jobs(dict(environment=self.env_name()))
            if not len(jobs):
                raise RuntimeError("No reference data for environment {}!".format(self.env_name()))

            reference = []
            generated = []
            for job in jobs:
                parameters = job.sp.parameters()
                if 'bundle' in parameters:
                    bundle = parameters.pop('bundle')
                    tmp_out = io.TextIOWrapper(
                        io.BytesIO(), sys.stdout.encoding)
                    with open(os.devnull, 'w') as devnull:
                        with redirect_stderr(devnull):
                            with redirect_stdout(tmp_out):
                                fp.submit(
                                    env=self.env, jobs=[job], names=bundle, pretend=True,
                                    force=True, bundle_size=len(bundle), **parameters)
                    tmp_out.seek(0)
                    msg = "---------- Bundled submission of job {}".format(job)
                    generated.extend([msg] + tmp_out.read().splitlines())

                    with open(job.fn('script_{}.sh'.format('_'.join(bundle)))) as file:
                        reference.extend([msg] + file.read().splitlines())
                else:
                    for op in fp.operations:
                        if 'partition' in parameters:
                            # Don't try to submit GPU operations to CPU partitions
                            # and vice versa.  We should be able to relax this
                            # requirement if we make our error checking more
                            # consistent.
                            if operator.xor(
                                'gpu' in parameters['partition'].lower(),
                                    'gpu' in op.lower()):
                                continue
                        tmp_out = io.TextIOWrapper(
                            io.BytesIO(), sys.stdout.encoding)
                        with open(os.devnull, 'w') as devnull:
                            with redirect_stderr(devnull):
                                with redirect_stdout(tmp_out):
                                    fp.submit(
                                        env=self.env, jobs=[job],
                                        names=[op], pretend=True, force=True, **parameters)
                        tmp_out.seek(0)
                        msg = "---------- Submission of operation {} for job {}.".format(op, job)
                        generated.extend([msg] + tmp_out.read().splitlines())

                        with open(job.fn('script_{}.sh'.format(op))) as file:
                            reference.extend([msg] + file.read().splitlines())

            self.assertEqual('\n'.join(reference), '\n'.join(generated))


# TestCase factory
for name, env in flow.environment.ComputeEnvironment.registry.items():
    if env.__module__.startswith('flow.environments'):
        name = '{}.{}'.format(env.__module__, env.__name__)[len('flow.'):]
        test_name = '{}TemplateTest'.format(env.__name__)
        test_cls = type(test_name, (BaseTemplateTest, unittest.TestCase), dict(env=env))
        locals()[test_name] = test_cls


if __name__ == '__main__':
    unittest.main()
