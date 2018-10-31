#!/bin/bash
#SBATCH --job-name="SubmissionTe/011496b3/mpi_op/0000/34b7d058861e0f88fed31de8a5d0f450"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(011496b3448205ee7d3e04cfe9adc7e4)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 011496b3448205ee7d3e04cfe9adc7e4

