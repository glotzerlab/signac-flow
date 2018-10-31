#!/bin/bash
#SBATCH --job-name="SubmissionTe/0a46314f/mpi_op/0000/1e4806b956ed043cf82278c07e6e3871"
#SBATCH --partition=compute
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(0a46314f3bb21140debbc8e4af120947)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 0a46314f3bb21140debbc8e4af120947

