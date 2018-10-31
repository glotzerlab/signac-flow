#!/bin/bash
#SBATCH --job-name="SubmissionTe/f433720d/serial_op/0000/7ca325da619933c93b76ce6e7d966847"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(f433720d93d195ee2026cd3934f015ff)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op f433720d93d195ee2026cd3934f015ff

