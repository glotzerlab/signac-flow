#!/bin/bash
#SBATCH --job-name="SubmissionTe/f90e4a81/hybrid_op/0000/d084b342338d8080381f818585cf2793"
#SBATCH --partition=shared
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(f90e4a81f388264f4050b3486a84377b)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op f90e4a81f388264f4050b3486a84377b

