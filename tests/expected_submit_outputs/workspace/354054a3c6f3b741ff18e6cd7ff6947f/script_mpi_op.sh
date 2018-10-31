#!/bin/bash
#SBATCH --job-name="SubmissionTe/354054a3/mpi_op/0000/13491fdf720f4de76457e93cc1a61293"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(354054a3c6f3b741ff18e6cd7ff6947f)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 354054a3c6f3b741ff18e6cd7ff6947f &
wait

