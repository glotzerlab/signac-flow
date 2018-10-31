#!/bin/bash
#SBATCH --job-name="SubmissionTe/de1e35d9/gpu_op/0000/df002f37fd6030a3e124b338280e2e0d"
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --gres=gpu:p100:2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(de1e35d92b83510d959ebfb4683d7cce)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op de1e35d92b83510d959ebfb4683d7cce

