#!/bin/bash
#SBATCH --job-name="SubmissionTe/66c927d2/omp_op/0000/0c4e1c044025f500181877d0480c7506"
#SBATCH --partition=RM-Shared
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(66c927d23507f3907b4cbded78a54f68)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 66c927d23507f3907b4cbded78a54f68

