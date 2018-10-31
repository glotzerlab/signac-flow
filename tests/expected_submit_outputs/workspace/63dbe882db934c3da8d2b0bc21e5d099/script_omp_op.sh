#!/bin/bash
#SBATCH --job-name="SubmissionTe/63dbe882/omp_op/0000/8a045452c8b43f85525f1b032ed0c279"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(63dbe882db934c3da8d2b0bc21e5d099)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 63dbe882db934c3da8d2b0bc21e5d099

