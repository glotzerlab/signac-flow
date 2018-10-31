#!/bin/bash
#SBATCH --job-name="SubmissionTe/0b5d7732/serial_op/0000/f1a806a1c634854f01733c2a53f691fe"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(0b5d77326769c7e5c65bdcf8c303265c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 0b5d77326769c7e5c65bdcf8c303265c

