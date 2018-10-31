#!/bin/bash
#SBATCH --job-name="SubmissionTe/ee92e792/omp_op/0000/a511c724fef0f0387c7bc4780bba42b6"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(ee92e7925c96192c2c6e2bdebf490652)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op ee92e7925c96192c2c6e2bdebf490652

