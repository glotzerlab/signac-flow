#!/bin/bash
#SBATCH --job-name="SubmissionTe/1d9e6021/omp_op/0000/dda36833667ee8c87868b94d6e5e8723"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(1d9e602187eaf2adadc60b0a6f6f6a60)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 1d9e602187eaf2adadc60b0a6f6f6a60 &
wait

