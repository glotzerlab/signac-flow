#!/bin/bash
#SBATCH --job-name="SubmissionTe/1d9e6021/hybrid_op/0000/d14db2adb3ca4e8c17224e02826c975a"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(1d9e602187eaf2adadc60b0a6f6f6a60)
export OMP_NUM_THREADS=2
ibrun -n 2 -o 0 task_affinity /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 1d9e602187eaf2adadc60b0a6f6f6a60 &
wait

