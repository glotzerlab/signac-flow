#!/bin/bash
#SBATCH --job-name="SubmissionTe/0b5d7732/parallel_op/0000/abd7c807cb1cd760fac033c9cf31578f"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(0b5d77326769c7e5c65bdcf8c303265c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 0b5d77326769c7e5c65bdcf8c303265c

