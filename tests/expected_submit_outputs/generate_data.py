# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import sys
from project import TestProject
import flow.environments
from flow import FlowProject

project = TestProject()

for job in project:
    with job:
        for op in project.operations:
            fn = 'script_{}.sh'.format(op)
            with open(fn, 'w') as f:
                sys.stdout = f
                kwargs = job.statepoint()
                env_spec = kwargs.pop('environment').split('.')
                env = getattr(getattr(flow.environments, env_spec[0]), env_spec[1])

                # I'm currently depending on the sequence to ensure that the "most
                # recent" present environment will be the one detected. This is
                # somewhat implementation specific, but it's baked deep into the way
                # flow's environment detection works so I'm OK with it for now.
                class TestEnvironment(env):
                    @classmethod
                    def is_present(self):
                        return True

                project.submit(names=[op.__name__], pretend=True, force=True, **kwargs)
