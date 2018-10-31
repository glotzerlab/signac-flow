#!/bin/bash
#SBATCH --job-name="SubmissionTe/eff95441/mpi_op/0000/6948881e1e2aa0094c012a15ff335a56"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(eff95441c00916227c0838e29ab01084)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op eff95441c00916227c0838e29ab01084

