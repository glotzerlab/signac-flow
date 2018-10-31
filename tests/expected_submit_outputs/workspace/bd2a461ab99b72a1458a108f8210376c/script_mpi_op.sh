#!/bin/bash
#SBATCH --job-name="SubmissionTe/bd2a461a/mpi_op/0000/cd9a0e9fd1e64242fac58aeb90944db9"
#SBATCH --partition=skx-normal
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(bd2a461ab99b72a1458a108f8210376c)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op bd2a461ab99b72a1458a108f8210376c

