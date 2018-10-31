#!/bin/bash
#SBATCH --job-name="SubmissionTe/ee92e792/serial_op/0000/f6e593f53122391c5bc856239b81d276"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(ee92e7925c96192c2c6e2bdebf490652)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op ee92e7925c96192c2c6e2bdebf490652

