#!/bin/bash
#SBATCH --job-name="SubmissionTe/216cd586/serial_op/0000/f1df6bdfc1f3ea134e9886768f7970ce"
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(216cd5860da17dc03ca7facd39d25e5c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 216cd5860da17dc03ca7facd39d25e5c

