#!/bin/bash
#SBATCH --job-name="SubmissionTe/216cd586/parallel_op/0000/5e756aa209f6598e9893c3e745e924c2"
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(216cd5860da17dc03ca7facd39d25e5c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 216cd5860da17dc03ca7facd39d25e5c

