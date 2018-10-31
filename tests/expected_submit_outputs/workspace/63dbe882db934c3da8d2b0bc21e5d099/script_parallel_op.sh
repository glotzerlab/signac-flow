#!/bin/bash
#SBATCH --job-name="SubmissionTe/63dbe882/parallel_op/0000/a12d764ce135d28f339b8369aa2282ba"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(63dbe882db934c3da8d2b0bc21e5d099)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 63dbe882db934c3da8d2b0bc21e5d099

