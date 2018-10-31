#!/bin/bash
#SBATCH --job-name="SubmissionTe/f433720d/hybrid_op/0000/0f0ea4340719aa0d9db01c146de0fa69"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(f433720d93d195ee2026cd3934f015ff)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op f433720d93d195ee2026cd3934f015ff

