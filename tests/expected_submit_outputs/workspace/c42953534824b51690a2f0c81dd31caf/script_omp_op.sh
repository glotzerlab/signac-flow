#!/bin/bash
#SBATCH --job-name="SubmissionTe/c4295353/omp_op/0000/6a78e521dcf20ee280d8368d1c57fdd2"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(c42953534824b51690a2f0c81dd31caf)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op c42953534824b51690a2f0c81dd31caf

