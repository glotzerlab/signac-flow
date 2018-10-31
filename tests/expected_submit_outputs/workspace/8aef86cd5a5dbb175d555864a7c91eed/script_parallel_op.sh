#!/bin/bash
#SBATCH --job-name="SubmissionTe/8aef86cd/parallel_op/0000/59c5aeafc49148932a1169fc71742602"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(8aef86cd5a5dbb175d555864a7c91eed)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 8aef86cd5a5dbb175d555864a7c91eed

