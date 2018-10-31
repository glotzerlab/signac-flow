#!/bin/bash
#SBATCH --job-name="SubmissionTe/c4295353/parallel_op/0000/284ffcea2496c9fd47f55bdee1be3288"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(c42953534824b51690a2f0c81dd31caf)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op c42953534824b51690a2f0c81dd31caf

