#!/bin/bash
#SBATCH --job-name="SubmissionTe/d53ea384/omp_op/0000/19d5b722caf68379829176ceffa8ca97"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(d53ea384c5f43fb64300e2591e2b2935)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op d53ea384c5f43fb64300e2591e2b2935

