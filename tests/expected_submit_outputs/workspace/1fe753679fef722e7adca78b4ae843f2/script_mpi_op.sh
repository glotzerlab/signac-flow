#!/bin/bash
#SBATCH --job-name="SubmissionTe/1fe75367/mpi_op/0000/c2427e7e768aa87bc2de27bab528ab28"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(1fe753679fef722e7adca78b4ae843f2)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 1fe753679fef722e7adca78b4ae843f2

