#!/bin/bash
#SBATCH --job-name="SubmissionTe/0b5d7732/mpi_op/0000/caf64c2541b36bc3f0842bb2ee0068b1"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(0b5d77326769c7e5c65bdcf8c303265c)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 0b5d77326769c7e5c65bdcf8c303265c

