#!/bin/bash
#SBATCH --job-name="SubmissionTe/eff95441/serial_op/0000/79432afb595debe55c04bbb985cfe6f4"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(eff95441c00916227c0838e29ab01084)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op eff95441c00916227c0838e29ab01084

