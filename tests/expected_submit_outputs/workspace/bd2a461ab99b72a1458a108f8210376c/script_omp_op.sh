#!/bin/bash
#SBATCH --job-name="SubmissionTe/bd2a461a/omp_op/0000/ddd98206ab00a4e6dbb0cc583b35960b"
#SBATCH --partition=skx-normal
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(bd2a461ab99b72a1458a108f8210376c)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op bd2a461ab99b72a1458a108f8210376c

