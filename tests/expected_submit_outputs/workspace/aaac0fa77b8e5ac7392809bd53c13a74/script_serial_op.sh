#!/bin/bash
#SBATCH --job-name="SubmissionTe/aaac0fa7/serial_op/0000/5b642bef96a4f3bb7ce157afca734eb7"
#SBATCH --partition=RM
#SBATCH -N 2
#SBATCH --ntasks-per-node 1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(aaac0fa77b8e5ac7392809bd53c13a74)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op aaac0fa77b8e5ac7392809bd53c13a74

