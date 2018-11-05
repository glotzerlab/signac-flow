# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function

import unittest

import sys
import re
import io
import operator

import signac
import flow
import flow.environments

import generate_data


class BaseTemplateTest(unittest.TestCase):
    project_class = signac.Project
    env = 'environment.UnknownEnvironment'

    def test_get_TestEnvironment(self):
        # Force asserts to show the full file when failures occur.
        # Useful to debug errors that arise.
        self.maxDiff = None

        # Must import the data into the project.
        with signac.TemporaryProject(
            name=generate_data.PROJECT_NAME,
            cls=generate_data.TestProject) as fp:
            fp.import_from(
                origin=generate_data.ARCHIVE_DIR)

            env = generate_data.get_nested_attr(flow, self.env)

            for job in fp.find_jobs(dict(environment=self.env)):
                parameters = job.sp.parameters
                if 'bundle' in parameters:
                    bundle = parameters.pop('bundle')
                    tmp_out = io.TextIOWrapper(
                        io.BytesIO(), sys.stdout.encoding)
                    with generate_data.redirect_stdout(tmp_out):
                        fp.submit(
                            env=env, jobs=[job], names=bundle, pretend=True,
                            force=True, bundle_size=len(bundle), **parameters)
                    tmp_out.seek(0)
                    generated = tmp_out.read()

                    fn = 'script_{}.sh'.format('_'.join(bundle))
                    with open(job.fn(fn)) as f:
                        self.assertEqual(
                            generate_data.mask_script(generated),
                            f.read(),
                            msg="Failed for bundle submission of job {}".format(
                                job.get_id()))
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
                        with generate_data.redirect_stdout(tmp_out):
                            fp.submit(
                                env=env, jobs=[job],
                                names=[op], pretend=True, force=True, **parameters)
                        tmp_out.seek(0)
                        generated = tmp_out.read()

                        fn = 'script_{}.sh'.format(op)
                        with open(job.fn(fn)) as f:
                            self.assertEqual(
                                generate_data.mask_script(generated),
                                f.read(),
                                msg="Failed for job {}, operation {}".format(
                                    job.get_id(), op))


class CometTemplateTest(BaseTemplateTest):
    env = 'environments.xsede.CometEnvironment'


class Stampede2TemplateTest(BaseTemplateTest):
    env = 'environments.xsede.Stampede2Environment'


class BridgesTemplateTest(BaseTemplateTest):
    env = 'environments.xsede.BridgesEnvironment'


class FluxTemplateTest(BaseTemplateTest):
    env = 'environments.umich.FluxEnvironment'


class TitanTemplateTest(BaseTemplateTest):
    env = 'environments.incite.TitanEnvironment'


class EosTemplateTest(BaseTemplateTest):
    env = 'environments.incite.EosEnvironment'


if __name__ == '__main__':
    unittest.main()
