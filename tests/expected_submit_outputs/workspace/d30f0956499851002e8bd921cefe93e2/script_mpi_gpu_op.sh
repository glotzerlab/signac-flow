#!/bin/bash
#SBATCH --job-name="SubmissionTe/d30f0956/mpi_gpu_op/0000/37672b99317f722246021ef4ed49c193"
#SBATCH --partition=GPU
#SBATCH -N 1
#SBATCH --ntasks-per-node 2
#SBATCH --gres=gpu:p100:2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(d30f0956499851002e8bd921cefe93e2)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op d30f0956499851002e8bd921cefe93e2

