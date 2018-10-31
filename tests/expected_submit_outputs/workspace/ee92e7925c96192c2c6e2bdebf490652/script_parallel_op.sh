#!/bin/bash
#SBATCH --job-name="SubmissionTe/ee92e792/parallel_op/0000/30bba4e33a7baf1e853f1d3a7d66d9f6"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(ee92e7925c96192c2c6e2bdebf490652)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op ee92e7925c96192c2c6e2bdebf490652

