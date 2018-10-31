#!/bin/bash
#SBATCH --job-name="SubmissionTe/216cd586/mpi_op/0000/97ff274e86c93b26d0cf2c66b78b5650"
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(216cd5860da17dc03ca7facd39d25e5c)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 216cd5860da17dc03ca7facd39d25e5c

