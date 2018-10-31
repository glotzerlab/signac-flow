#!/bin/bash
#SBATCH --job-name="SubmissionTe/bd2a461a/serial_op/0000/c6495a3ee9bbb49de211b20eacdfa5fd"
#SBATCH --partition=skx-normal
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(bd2a461ab99b72a1458a108f8210376c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op bd2a461ab99b72a1458a108f8210376c

