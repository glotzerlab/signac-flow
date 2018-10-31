#!/bin/bash
#SBATCH --job-name="SubmissionTe/f3fa47ca/mpi_op/0000/97c09da6b52a72414a5cd0831c09aec5"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(f3fa47caf345d14c4c9ccb604b76f2e3)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op f3fa47caf345d14c4c9ccb604b76f2e3

