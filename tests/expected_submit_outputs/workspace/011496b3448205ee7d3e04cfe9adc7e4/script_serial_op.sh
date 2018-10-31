#!/bin/bash
#SBATCH --job-name="SubmissionTe/011496b3/serial_op/0000/25fde8168d4143bdf25c57a701691209"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(011496b3448205ee7d3e04cfe9adc7e4)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 011496b3448205ee7d3e04cfe9adc7e4

