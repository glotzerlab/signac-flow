#!/bin/bash
#SBATCH --job-name="SubmissionTest/bundle/f1e66e0dfc204b8e0061d396b7d6b69bb7b0ef22"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(abd47a2fb2ca273ba881932a57fdc4d7)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op abd47a2fb2ca273ba881932a57fdc4d7

# omp_op(abd47a2fb2ca273ba881932a57fdc4d7)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op abd47a2fb2ca273ba881932a57fdc4d7

