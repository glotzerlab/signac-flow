# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.

import signac
import flow
import flow.environments
import itertools

def cartesian(**kwargs):
    for combo in itertools.product(*kwargs.values()):
        yield dict(zip(kwargs.keys(), combo))

def main():
    project = signac.init_project('SubmissionTest')
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


if __name__ == "__main__":
    main()


