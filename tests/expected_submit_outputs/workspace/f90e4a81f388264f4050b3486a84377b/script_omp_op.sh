#!/bin/bash
#SBATCH --job-name="SubmissionTe/f90e4a81/omp_op/0000/6c7e68b95bbc3d050190cc092e9aa4ba"
#SBATCH --partition=shared
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(f90e4a81f388264f4050b3486a84377b)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op f90e4a81f388264f4050b3486a84377b

