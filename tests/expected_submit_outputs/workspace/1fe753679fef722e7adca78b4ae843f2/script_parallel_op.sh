#!/bin/bash
#SBATCH --job-name="SubmissionTe/1fe75367/parallel_op/0000/3923f5bcea8aa2375beb1cc0ec0d62b5"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(1fe753679fef722e7adca78b4ae843f2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 1fe753679fef722e7adca78b4ae843f2

