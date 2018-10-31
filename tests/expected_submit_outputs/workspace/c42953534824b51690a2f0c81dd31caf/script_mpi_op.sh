#!/bin/bash
#SBATCH --job-name="SubmissionTe/c4295353/mpi_op/0000/f5f01d21cf30c0d12068379182c5069b"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(c42953534824b51690a2f0c81dd31caf)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op c42953534824b51690a2f0c81dd31caf

