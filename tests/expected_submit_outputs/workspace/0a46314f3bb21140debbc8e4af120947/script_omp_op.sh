#!/bin/bash
#SBATCH --job-name="SubmissionTe/0a46314f/omp_op/0000/ed53f10bd7a8a2a214e4ea780a7b0007"
#SBATCH --partition=compute
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(0a46314f3bb21140debbc8e4af120947)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 0a46314f3bb21140debbc8e4af120947

