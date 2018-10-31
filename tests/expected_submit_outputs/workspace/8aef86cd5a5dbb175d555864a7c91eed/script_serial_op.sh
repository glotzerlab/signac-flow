#!/bin/bash
#SBATCH --job-name="SubmissionTe/8aef86cd/serial_op/0000/21efc92662cc28cf96ec479e9571a165"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(8aef86cd5a5dbb175d555864a7c91eed)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 8aef86cd5a5dbb175d555864a7c91eed

