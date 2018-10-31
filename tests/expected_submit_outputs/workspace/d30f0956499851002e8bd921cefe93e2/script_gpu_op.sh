#!/bin/bash
#SBATCH --job-name="SubmissionTe/d30f0956/gpu_op/0000/f8d86ee758b085557e200f959485a0eb"
#SBATCH --partition=GPU
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --gres=gpu:p100:2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(d30f0956499851002e8bd921cefe93e2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op d30f0956499851002e8bd921cefe93e2

