#!/bin/bash
#SBATCH --job-name="SubmissionTe/f433720d/omp_op/0000/01718c940fd301fdf4864b0745708e40"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(f433720d93d195ee2026cd3934f015ff)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op f433720d93d195ee2026cd3934f015ff

