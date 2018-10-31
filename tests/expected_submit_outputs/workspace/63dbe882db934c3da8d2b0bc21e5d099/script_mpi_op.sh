#!/bin/bash
#SBATCH --job-name="SubmissionTe/63dbe882/mpi_op/0000/e6605ea42af9f8a7a1c268d987a15905"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(63dbe882db934c3da8d2b0bc21e5d099)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 63dbe882db934c3da8d2b0bc21e5d099

