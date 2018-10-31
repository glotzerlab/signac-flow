#!/bin/bash
#SBATCH --job-name="SubmissionTe/b2c00b6e/gpu_op/0000/e1ca6c011042c481928e5941c59da317"
#SBATCH --partition=gpu
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:p100:2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(b2c00b6e5630ad7c40eb9f7ce4f0576c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op b2c00b6e5630ad7c40eb9f7ce4f0576c

