#!/bin/bash
#SBATCH --job-name="SubmissionTe/f433720d/parallel_op/0000/8ae945039facdebc60925404b021152d"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(f433720d93d195ee2026cd3934f015ff)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op f433720d93d195ee2026cd3934f015ff

