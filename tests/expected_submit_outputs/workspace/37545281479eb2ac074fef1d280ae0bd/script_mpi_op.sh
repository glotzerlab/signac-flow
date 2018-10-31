#!/bin/bash
#SBATCH --job-name="SubmissionTe/37545281/mpi_op/0000/2d4bd341fbcc2b4072efeed035319078"
#SBATCH --partition=skx-normal
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(37545281479eb2ac074fef1d280ae0bd)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 37545281479eb2ac074fef1d280ae0bd

