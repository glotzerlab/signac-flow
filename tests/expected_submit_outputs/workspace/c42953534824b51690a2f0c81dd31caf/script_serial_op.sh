#!/bin/bash
#SBATCH --job-name="SubmissionTe/c4295353/serial_op/0000/7913d3cb05fafd3e1db92ad9836e7484"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(c42953534824b51690a2f0c81dd31caf)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op c42953534824b51690a2f0c81dd31caf

