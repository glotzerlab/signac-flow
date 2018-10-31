#!/bin/bash
#SBATCH --job-name="SubmissionTe/eff95441/omp_op/0000/1c1464382c0b9a9351e325aa8750c7dc"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(eff95441c00916227c0838e29ab01084)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op eff95441c00916227c0838e29ab01084

