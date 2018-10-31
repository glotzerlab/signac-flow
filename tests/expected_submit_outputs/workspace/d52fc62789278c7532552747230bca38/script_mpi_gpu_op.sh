#!/bin/bash
#SBATCH --job-name="SubmissionTe/d52fc627/mpi_gpu_op/0000/3bfed85d64a6f2700392f05fd3e5de9b"
#SBATCH --partition=GPU
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 2
#SBATCH --gres=gpu:p100:2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(d52fc62789278c7532552747230bca38)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op d52fc62789278c7532552747230bca38

