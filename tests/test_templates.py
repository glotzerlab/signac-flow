# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function

import unittest

import sys
import os
import re
import io
import operator

import signac
import flow
import flow.environments

from generate_data import (
    get_nested_attr, redirect_stdout, TestProject,
    PROJECT_NAME, ARCHIVE_DIR, NAME_REGEX)


class BaseTemplateTest(unittest.TestCase):
    project_class = signac.Project
    env = 'environment.UnknownEnvironment'

    def test_get_TestEnvironment(self):
        # Force asserts to show the full file when failures occur.
        # Useful to debug errors that arise.
        self.maxDiff = None

        # Must import the data into the project.
        with signac.contrib.TemporaryProject(name=PROJECT_NAME) as reference_project:
            reference_project.import_from(
                origin=ARCHIVE_DIR)
            fp = TestProject.get_project(
                root=reference_project.root_directory())

            env = get_nested_attr(flow, self.env)

            for job in fp.find_jobs(dict(environment=self.env)):
                parameters = job.sp.parameters
                if 'bundle' in parameters:
                    bundle = parameters.pop('bundle')
                    tmp_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
                    new_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
                    with redirect_stdout(tmp_out):
                        fp.submit(
                            env=env, jobs=[job], names=bundle, pretend=True,
                            force=True, bundle_size=len(bundle), **parameters)
                    tmp_out.seek(0)

                    for line in tmp_out:
                        if '#PBS' in line or '#SBATCH' in line or 'OMP_NUM_THREADS' in line:
                            if '#PBS -N' in line or '#SBATCH --job-name' in line:
                                match = re.match(NAME_REGEX, line)
                                new_out.write(match.group(1) + '\n')
                            else:
                                new_out.write(line)

                    new_out.seek(0)
                    generated = new_out.read()

                    fn = 'script_{}.sh'.format('_'.join(bundle))
                    with open(job.fn(fn)) as f:
                        self.assertEqual(generated, f.read())
                else:
                    for op in fp.operations:
                        if 'partition' in parameters:
                            # Don't try to submit GPU operations to CPU partitions
                            # and vice versa.  We should be able to relax this
                            # requirement if we make our error checking more
                            # consistent.
                            if operator.xor('gpu' in parameters['partition'].lower(), 'gpu' in op.lower()):
                                    continue
                        tmp_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
                        new_out = io.TextIOWrapper(io.BytesIO(), sys.stdout.encoding)
                        with redirect_stdout(tmp_out):
                            fp.submit(
                                env=env, jobs=[job],
                                names=[op], pretend=True, force=True, **parameters)
                        tmp_out.seek(0)

                        for line in tmp_out:
                            if '#PBS' in line or '#SBATCH' in line or 'OMP_NUM_THREADS' in line:
                                if '#PBS -N' in line or '#SBATCH --job-name' in line:
                                    match = re.match(NAME_REGEX, line)
                                    new_out.write(match.group(1) + '\n')
                                else:
                                    new_out.write(line)

                        new_out.seek(0)
                        generated = new_out.read()
                        fn = 'script_{}.sh'.format(op)
                        with open(job.fn(fn)) as f:
                            self.assertEqual(generated, f.read())


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
