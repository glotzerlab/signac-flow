#!/bin/bash
#SBATCH --job-name="SubmissionTe/354054a3/parallel_op/0000/6324af82747ca5ff01d43d8972697cd3"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(354054a3c6f3b741ff18e6cd7ff6947f)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 354054a3c6f3b741ff18e6cd7ff6947f &
wait

