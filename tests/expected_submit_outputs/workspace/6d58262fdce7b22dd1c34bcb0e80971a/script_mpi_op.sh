#!/bin/bash
#SBATCH --job-name="SubmissionTe/6d58262f/mpi_op/0000/b0d55d3b23aca7a4fdd7b943026b4cb5"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(6d58262fdce7b22dd1c34bcb0e80971a)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 6d58262fdce7b22dd1c34bcb0e80971a &
wait

