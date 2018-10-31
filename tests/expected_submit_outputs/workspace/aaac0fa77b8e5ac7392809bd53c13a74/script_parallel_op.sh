#!/bin/bash
#SBATCH --job-name="SubmissionTe/aaac0fa7/parallel_op/0000/69bbd1b34aad679cd4148a25a3c70830"
#SBATCH --partition=RM
#SBATCH -N 2
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(aaac0fa77b8e5ac7392809bd53c13a74)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op aaac0fa77b8e5ac7392809bd53c13a74

