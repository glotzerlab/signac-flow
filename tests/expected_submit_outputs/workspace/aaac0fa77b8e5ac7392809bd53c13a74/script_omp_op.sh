#!/bin/bash
#SBATCH --job-name="SubmissionTe/aaac0fa7/omp_op/0000/2f6641bee908d501db09eb173bde864a"
#SBATCH --partition=RM
#SBATCH -N 2
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(aaac0fa77b8e5ac7392809bd53c13a74)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op aaac0fa77b8e5ac7392809bd53c13a74

