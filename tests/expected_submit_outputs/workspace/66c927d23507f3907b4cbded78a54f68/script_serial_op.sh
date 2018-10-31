#!/bin/bash
#SBATCH --job-name="SubmissionTe/66c927d2/serial_op/0000/d233256e367c43ba2283212a41a722d6"
#SBATCH --partition=RM-Shared
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(66c927d23507f3907b4cbded78a54f68)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 66c927d23507f3907b4cbded78a54f68

