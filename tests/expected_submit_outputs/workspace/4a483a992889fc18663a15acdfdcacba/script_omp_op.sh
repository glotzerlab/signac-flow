#!/bin/bash
#SBATCH --job-name="SubmissionTe/4a483a99/omp_op/0000/1cf6c184207c1b3df567c8c28876cc97"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(4a483a992889fc18663a15acdfdcacba)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 4a483a992889fc18663a15acdfdcacba

