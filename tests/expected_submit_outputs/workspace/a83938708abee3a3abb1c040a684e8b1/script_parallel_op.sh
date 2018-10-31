#!/bin/bash
#SBATCH --job-name="SubmissionTe/a8393870/parallel_op/0000/8cbdd7d9fc1c55cf0d42ddd6c7aad475"
#SBATCH --partition=RM
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(a83938708abee3a3abb1c040a684e8b1)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op a83938708abee3a3abb1c040a684e8b1

