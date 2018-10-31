#!/bin/bash
#SBATCH --job-name="SubmissionTe/d53ea384/serial_op/0000/9ebe2a584e9f89e27e67e5f7412ca922"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(d53ea384c5f43fb64300e2591e2b2935)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op d53ea384c5f43fb64300e2591e2b2935

