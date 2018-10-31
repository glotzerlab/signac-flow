#!/bin/bash
#SBATCH --job-name="SubmissionTe/0b5d7732/omp_op/0000/ca92a2173d96495f44d3918749f2a1a1"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(0b5d77326769c7e5c65bdcf8c303265c)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 0b5d77326769c7e5c65bdcf8c303265c

