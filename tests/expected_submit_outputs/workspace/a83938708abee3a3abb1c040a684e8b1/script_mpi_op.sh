#!/bin/bash
#SBATCH --job-name="SubmissionTe/a8393870/mpi_op/0000/557524873b6a7b9bdf3f7b5435b5e959"
#SBATCH --partition=RM
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(a83938708abee3a3abb1c040a684e8b1)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op a83938708abee3a3abb1c040a684e8b1

