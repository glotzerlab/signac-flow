#!/bin/bash
#SBATCH --job-name="SubmissionTe/d52fc627/gpu_op/0000/3f25f455b5e57f9c195cb05a263f539c"
#SBATCH --partition=GPU
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 1
#SBATCH --gres=gpu:p100:2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(d52fc62789278c7532552747230bca38)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op d52fc62789278c7532552747230bca38

