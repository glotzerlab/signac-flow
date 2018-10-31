#!/bin/bash
#SBATCH --job-name="SubmissionTe/eff95441/parallel_op/0000/41765560f3b71c9b3756343682dc988f"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(eff95441c00916227c0838e29ab01084)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op eff95441c00916227c0838e29ab01084

