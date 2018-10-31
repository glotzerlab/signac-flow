#!/bin/bash
#SBATCH --job-name="SubmissionTe/6d58262f/parallel_op/0000/f356da0bbbf8f0b8d314af3784783809"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(6d58262fdce7b22dd1c34bcb0e80971a)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 6d58262fdce7b22dd1c34bcb0e80971a &
wait

