#!/bin/bash
#SBATCH --job-name="SubmissionTe/66c927d2/mpi_op/0000/824dae5c38ecf1c17be5df0c63f11eb2"
#SBATCH --partition=RM-Shared
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(66c927d23507f3907b4cbded78a54f68)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 66c927d23507f3907b4cbded78a54f68

