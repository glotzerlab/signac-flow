#!/bin/bash
#SBATCH --job-name="SubmissionTe/4a483a99/mpi_op/0000/7925901de7a98b4d6159c93c6647e216"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(4a483a992889fc18663a15acdfdcacba)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 4a483a992889fc18663a15acdfdcacba

