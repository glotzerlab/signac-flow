#!/bin/bash
#SBATCH --job-name="SubmissionTe/1fe75367/hybrid_op/0000/428ee6f28db50fb451c9b42864199bf4"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(1fe753679fef722e7adca78b4ae843f2)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 1fe753679fef722e7adca78b4ae843f2

