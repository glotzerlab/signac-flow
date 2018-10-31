#!/bin/bash
#SBATCH --job-name="SubmissionTe/92c6054c/parallel_op/0000/7e8d056990ac238484fc33bf3dc4be0f"
#SBATCH --partition=RM-Shared
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(92c6054c6acae4abd09b0055afdf157f)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 92c6054c6acae4abd09b0055afdf157f

