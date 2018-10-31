#!/bin/bash
#SBATCH --job-name="SubmissionTe/0b5d7732/hybrid_op/0000/f85bfbf8993dbd61b8cf100b2df8b930"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(0b5d77326769c7e5c65bdcf8c303265c)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 0b5d77326769c7e5c65bdcf8c303265c

