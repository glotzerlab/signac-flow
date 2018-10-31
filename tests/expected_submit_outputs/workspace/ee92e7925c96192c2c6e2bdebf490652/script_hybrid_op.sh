#!/bin/bash
#SBATCH --job-name="SubmissionTe/ee92e792/hybrid_op/0000/9e29d2c240a0b0bdf268127456658d82"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(ee92e7925c96192c2c6e2bdebf490652)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op ee92e7925c96192c2c6e2bdebf490652

