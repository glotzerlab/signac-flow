#!/bin/bash
#SBATCH --job-name="SubmissionTe/92c6054c/mpi_op/0000/d6e9d5062fed615c290b5cc780bbcfaf"
#SBATCH --partition=RM-Shared
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(92c6054c6acae4abd09b0055afdf157f)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 92c6054c6acae4abd09b0055afdf157f

