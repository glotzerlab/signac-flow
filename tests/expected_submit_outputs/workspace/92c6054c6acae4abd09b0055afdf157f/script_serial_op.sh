#!/bin/bash
#SBATCH --job-name="SubmissionTe/92c6054c/serial_op/0000/b82465d2d43f5909344934d5989288a6"
#SBATCH --partition=RM-Shared
#SBATCH -N 1
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(92c6054c6acae4abd09b0055afdf157f)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 92c6054c6acae4abd09b0055afdf157f

