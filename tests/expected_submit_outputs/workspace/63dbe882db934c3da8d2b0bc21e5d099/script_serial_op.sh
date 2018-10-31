#!/bin/bash
#SBATCH --job-name="SubmissionTe/63dbe882/serial_op/0000/061eca94b55f13e55c3ef27622e4eda4"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(63dbe882db934c3da8d2b0bc21e5d099)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 63dbe882db934c3da8d2b0bc21e5d099

