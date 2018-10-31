#!/bin/bash
#SBATCH --job-name="SubmissionTe/0a46314f/hybrid_op/0000/fac492d9a564d5cd670efa0b65ca7a59"
#SBATCH --partition=compute
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(0a46314f3bb21140debbc8e4af120947)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 0a46314f3bb21140debbc8e4af120947

