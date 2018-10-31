#!/bin/bash
#SBATCH --job-name="SubmissionTe/66c927d2/parallel_op/0000/453d16a5274f64537352b83319c23a1e"
#SBATCH --partition=RM-Shared
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(66c927d23507f3907b4cbded78a54f68)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 66c927d23507f3907b4cbded78a54f68

