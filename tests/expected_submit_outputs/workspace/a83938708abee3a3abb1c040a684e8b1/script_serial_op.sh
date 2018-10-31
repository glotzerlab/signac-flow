#!/bin/bash
#SBATCH --job-name="SubmissionTe/a8393870/serial_op/0000/0e53bbc9a64b9c12a9b0b773e8e399e1"
#SBATCH --partition=RM
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(a83938708abee3a3abb1c040a684e8b1)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op a83938708abee3a3abb1c040a684e8b1

