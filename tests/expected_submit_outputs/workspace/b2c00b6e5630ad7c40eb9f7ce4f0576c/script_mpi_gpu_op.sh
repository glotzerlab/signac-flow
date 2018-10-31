#!/bin/bash
#SBATCH --job-name="SubmissionTe/b2c00b6e/mpi_gpu_op/0000/f494672329c40dce4a371a61ee7ab83b"
#SBATCH --partition=gpu
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:p100:2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(b2c00b6e5630ad7c40eb9f7ce4f0576c)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op b2c00b6e5630ad7c40eb9f7ce4f0576c

