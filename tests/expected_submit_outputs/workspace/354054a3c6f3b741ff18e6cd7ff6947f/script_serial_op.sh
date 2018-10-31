#!/bin/bash
#SBATCH --job-name="SubmissionTe/354054a3/serial_op/0000/86a95846bdb38f86f558a7ad47bd98b8"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(354054a3c6f3b741ff18e6cd7ff6947f)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 354054a3c6f3b741ff18e6cd7ff6947f &
wait

