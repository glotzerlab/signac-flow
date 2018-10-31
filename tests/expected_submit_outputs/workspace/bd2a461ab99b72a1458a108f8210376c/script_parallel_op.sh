#!/bin/bash
#SBATCH --job-name="SubmissionTe/bd2a461a/parallel_op/0000/6cdee5cc423fabce8798a87042a86e3a"
#SBATCH --partition=skx-normal
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(bd2a461ab99b72a1458a108f8210376c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op bd2a461ab99b72a1458a108f8210376c

