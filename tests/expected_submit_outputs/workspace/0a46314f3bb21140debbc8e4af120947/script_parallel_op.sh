#!/bin/bash
#SBATCH --job-name="SubmissionTe/0a46314f/parallel_op/0000/7e6544cfa020dd6df95bdcba44e2a262"
#SBATCH --partition=compute
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(0a46314f3bb21140debbc8e4af120947)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 0a46314f3bb21140debbc8e4af120947

