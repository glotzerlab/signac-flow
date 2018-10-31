#!/bin/bash
#SBATCH --job-name="SubmissionTest/bundle/9f9ae3aafdae952831f5e9fbd2b2f5ecf16f672f"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(a5e3b313aaf6e78c0f4dd8b58e83949e)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op a5e3b313aaf6e78c0f4dd8b58e83949e &

# omp_op(a5e3b313aaf6e78c0f4dd8b58e83949e)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op a5e3b313aaf6e78c0f4dd8b58e83949e &
wait

