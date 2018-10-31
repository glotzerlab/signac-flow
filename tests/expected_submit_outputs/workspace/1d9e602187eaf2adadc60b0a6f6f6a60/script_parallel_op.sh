#!/bin/bash
#SBATCH --job-name="SubmissionTe/1d9e6021/parallel_op/0000/04e08c17588b8f3b756c94971193e7bc"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(1d9e602187eaf2adadc60b0a6f6f6a60)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 1d9e602187eaf2adadc60b0a6f6f6a60 &
wait

