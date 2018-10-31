#!/bin/bash
#SBATCH --job-name="SubmissionTe/d53ea384/parallel_op/0000/1f9e1dd421560a34ed73172d5175a027"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(d53ea384c5f43fb64300e2591e2b2935)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op d53ea384c5f43fb64300e2591e2b2935

