#!/bin/bash
#SBATCH --job-name="SubmissionTe/de1e35d9/mpi_gpu_op/0000/dce6e8521c3131fac835468a4da64e82"
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --gres=gpu:p100:2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(de1e35d92b83510d959ebfb4683d7cce)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op de1e35d92b83510d959ebfb4683d7cce

