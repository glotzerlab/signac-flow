#!/bin/bash
#SBATCH --job-name="SubmissionTe/1d9e6021/serial_op/0000/5bf58f269c3e260b985642468c9dd4a4"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(1d9e602187eaf2adadc60b0a6f6f6a60)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 1d9e602187eaf2adadc60b0a6f6f6a60 &
wait

