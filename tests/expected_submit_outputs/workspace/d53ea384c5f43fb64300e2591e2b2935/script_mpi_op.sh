#!/bin/bash
#SBATCH --job-name="SubmissionTe/d53ea384/mpi_op/0000/4859eebfabcb28df3f7f08ca2927da10"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(d53ea384c5f43fb64300e2591e2b2935)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op d53ea384c5f43fb64300e2591e2b2935

