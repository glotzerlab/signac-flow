#!/bin/bash
#SBATCH --job-name="SubmissionTe/6d58262f/omp_op/0000/6dd94f9bba0203aa99ed1a59e3ca68b5"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(6d58262fdce7b22dd1c34bcb0e80971a)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 6d58262fdce7b22dd1c34bcb0e80971a &
wait

