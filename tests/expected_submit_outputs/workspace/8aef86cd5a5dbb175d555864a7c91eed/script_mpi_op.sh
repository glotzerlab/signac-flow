#!/bin/bash
#SBATCH --job-name="SubmissionTe/8aef86cd/mpi_op/0000/c8864260a3ff2373793f6a72ea62efba"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(8aef86cd5a5dbb175d555864a7c91eed)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 8aef86cd5a5dbb175d555864a7c91eed

