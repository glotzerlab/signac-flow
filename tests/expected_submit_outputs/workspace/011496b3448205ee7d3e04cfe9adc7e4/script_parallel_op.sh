#!/bin/bash
#SBATCH --job-name="SubmissionTe/011496b3/parallel_op/0000/f71a514302ff1fb14dbe42f880bbd7bf"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(011496b3448205ee7d3e04cfe9adc7e4)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 011496b3448205ee7d3e04cfe9adc7e4

