#!/bin/bash
#SBATCH --job-name="SubmissionTe/f3fa47ca/parallel_op/0000/fa9220a6f32b253b9a0274490987e85b"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(f3fa47caf345d14c4c9ccb604b76f2e3)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op f3fa47caf345d14c4c9ccb604b76f2e3

