# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import unittest

import signac
import six
import flow.environments
import flow.environment
from expected_submit_outputs.project import TestProject
from generate_data import get_nested_attr
import sys
import os
from io import TextIOWrapper, BytesIO

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory


class BaseTemplateTest(unittest.TestCase):
    project_class = signac.Project
    env = 'environment.UnknownEnvironment'

    def test_get_TestEnvironment(self):
        reference_project = TestProject.get_project(
            root=os.path.join(
                os.path.dirname(__file__),
                './expected_submit_outputs')
            )

        orig_stdout = sys.stdout
        env=get_nested_attr(flow, self.env)
        for job in reference_project.find_jobs(dict(environment=self.env)):
            parameters = job.sp.parameters
            new_out = TextIOWrapper(BytesIO(), sys.stdout.encoding)
            sys.stdout = new_out
            if 'bundle' not in parameters:
                for op in reference_project.operations:
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
                    reference_project.submit(
                        env=env, jobs=[job],
                        names=[op], pretend=True, force=True, **parameters)
                    new_out.seek(0)
                    generated = new_out.read()
                    fn = 'script_{}.sh'.format(op)
                    with open(job.fn(fn)) as f:
                        reference = f.read()
                    self.assertTrue(generated, reference)
            else:
                bundle = parameters.pop('bundle')
                reference_project.submit(
                    env=env, jobs=[job], names=bundle, pretend=True,
                    force=True, bundle_size=len(bundle), **parameters)
                new_out.seek(0)
                generated = new_out.read()

                fn = 'script_{}.sh'.format('_'.join(bundle))
                with open(job.fn(fn)) as f:
                    reference = f.read()
                self.assertTrue(generated, reference)

        sys.stdout = orig_stdout


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
