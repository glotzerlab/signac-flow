# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function

import unittest

import signac
import six
import flow.environments
import flow.environment
from expected_submit_outputs.project import TestProject
from generate_data import get_nested_attr, redirect_stdout
import sys
import os
from io import TextIOWrapper, BytesIO
import re

if six.PY2:
    from tempdir import TemporaryDirectory
else:
    from tempfile import TemporaryDirectory



class BaseTemplateTest(unittest.TestCase):
    project_class = signac.Project
    env = 'environment.UnknownEnvironment'

    def test_get_TestEnvironment(self):
        self.maxDiff = None

        # This regex will be used to filter out the final hash in the job name.
        name_regex = r'(.*)\/[a-z0-9]*'

        reference_project = TestProject.get_project(
            root=os.path.join(
                os.path.dirname(__file__),
                './expected_submit_outputs')
            )

        env = get_nested_attr(flow, self.env)
        for job in reference_project.find_jobs(dict(environment=self.env)):
            parameters = job.sp.parameters
            if 'bundle' in parameters:
                bundle = parameters.pop('bundle')
                tmp_out = TextIOWrapper(BytesIO(), sys.stdout.encoding)
                new_out = TextIOWrapper(BytesIO(), sys.stdout.encoding)
                with redirect_stdout(tmp_out):
                    reference_project.submit(
                        env=env, jobs=[job], names=bundle, pretend=True,
                        force=True, bundle_size=len(bundle), **parameters)
                tmp_out.seek(0)

                for line in tmp_out:
                    if '#PBS' in line or '#SBATCH' in line or 'OMP_NUM_THREADS' in line:
                        if '#PBS -N' in line or '#SBATCH --job-name' in line:
                            match = re.match(name_regex, line)
                            new_out.write(match.group(1) + '\n')
                        else:
                            new_out.write(line)

                new_out.seek(0)
                generated = new_out.read()

                fn = 'script_{}.sh'.format('_'.join(bundle))
                with open(job.fn(fn)) as f:
                    self.assertEqual(generated, f.read())
            else:
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
                    tmp_out = TextIOWrapper(BytesIO(), sys.stdout.encoding)
                    new_out = TextIOWrapper(BytesIO(), sys.stdout.encoding)
                    with redirect_stdout(tmp_out):
                        reference_project.submit(
                            env=env, jobs=[job],
                            names=[op], pretend=True, force=True, **parameters)
                    tmp_out.seek(0)

                    for line in tmp_out:
                        if '#PBS' in line or '#SBATCH' in line or 'OMP_NUM_THREADS' in line:
                            if '#PBS -N' in line or '#SBATCH --job-name' in line:
                                match = re.match(name_regex, line)
                                new_out.write(match.group(1) + '\n')
                            else:
                                new_out.write(line)

                    new_out.seek(0)
                    generated = new_out.read()
                    fn = 'script_{}.sh'.format(op)
                    with open(job.fn(fn)) as f:
                        self.assertEqual(generated, f.read())
            break


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
