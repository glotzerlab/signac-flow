#!/bin/bash
#SBATCH --job-name="SubmissionTe/aaac0fa7/mpi_op/0000/b71a007253a08ce491765925bdeaea0b"
#SBATCH --partition=RM
#SBATCH -N 2
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(aaac0fa77b8e5ac7392809bd53c13a74)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op aaac0fa77b8e5ac7392809bd53c13a74

