#!/bin/bash
#SBATCH --job-name="SubmissionTe/e5515eac/parallel_op/0000/c9db997a394ac644b449f60726660055"
#SBATCH --partition=compute
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(e5515eac081f886786169793e9b96512)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op e5515eac081f886786169793e9b96512

