#!/bin/bash
#SBATCH --job-name="SubmissionTe/c4295353/hybrid_op/0000/ffd096fc1e65e39bd6dd31c3f22c7366"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(c42953534824b51690a2f0c81dd31caf)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op c42953534824b51690a2f0c81dd31caf

