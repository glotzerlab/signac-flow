#!/bin/bash
#SBATCH --job-name="SubmissionTe/f90e4a81/serial_op/0000/605298f8d1aab737d3242f70e03a09f0"
#SBATCH --partition=shared
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(f90e4a81f388264f4050b3486a84377b)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op f90e4a81f388264f4050b3486a84377b

