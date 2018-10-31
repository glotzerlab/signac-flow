#!/bin/bash
#SBATCH --job-name="SubmissionTe/66c927d2/hybrid_op/0000/8dde07c018ee8fe4509b2b1726da751c"
#SBATCH --partition=RM-Shared
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(66c927d23507f3907b4cbded78a54f68)
export OMP_NUM_THREADS=2
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 66c927d23507f3907b4cbded78a54f68

