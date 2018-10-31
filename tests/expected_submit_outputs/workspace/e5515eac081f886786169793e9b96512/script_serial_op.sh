#!/bin/bash
#SBATCH --job-name="SubmissionTe/e5515eac/serial_op/0000/d7f4d867a5f15c4121ae92de7a9a1903"
#SBATCH --partition=compute
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(e5515eac081f886786169793e9b96512)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op e5515eac081f886786169793e9b96512

