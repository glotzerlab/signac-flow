#!/bin/bash
#SBATCH --job-name="SubmissionTe/6d58262f/serial_op/0000/b9a8357751d1d5443f1a5145fac5a42c"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(6d58262fdce7b22dd1c34bcb0e80971a)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 6d58262fdce7b22dd1c34bcb0e80971a &
wait

