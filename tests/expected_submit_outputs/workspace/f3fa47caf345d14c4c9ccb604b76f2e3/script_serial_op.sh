#!/bin/bash
#SBATCH --job-name="SubmissionTe/f3fa47ca/serial_op/0000/8763ff76c7fc9ba17efabe644335ff7d"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(f3fa47caf345d14c4c9ccb604b76f2e3)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op f3fa47caf345d14c4c9ccb604b76f2e3

