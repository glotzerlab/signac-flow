#!/bin/bash
#SBATCH --job-name="SubmissionTe/f90e4a81/mpi_op/0000/fc9f2686ff141c3e8426d1c3c4c4d4ca"
#SBATCH --partition=shared
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(f90e4a81f388264f4050b3486a84377b)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op f90e4a81f388264f4050b3486a84377b

