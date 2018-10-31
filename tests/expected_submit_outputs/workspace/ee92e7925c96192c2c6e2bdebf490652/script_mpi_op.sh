#!/bin/bash
#SBATCH --job-name="SubmissionTe/ee92e792/mpi_op/0000/b5ee8575f06d55cb5f3c3a7638309b10"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(ee92e7925c96192c2c6e2bdebf490652)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op ee92e7925c96192c2c6e2bdebf490652

