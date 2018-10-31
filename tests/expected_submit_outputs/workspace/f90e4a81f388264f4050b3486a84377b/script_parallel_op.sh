#!/bin/bash
#SBATCH --job-name="SubmissionTe/f90e4a81/parallel_op/0000/e2dc1fa2be7db5a2427da025c476e85f"
#SBATCH --partition=shared
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(f90e4a81f388264f4050b3486a84377b)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op f90e4a81f388264f4050b3486a84377b

