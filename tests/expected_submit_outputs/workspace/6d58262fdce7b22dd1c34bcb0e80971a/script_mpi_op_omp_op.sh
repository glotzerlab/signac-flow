#!/bin/bash
#SBATCH --job-name="SubmissionTest/bundle/8e0d31a0a19879aae84f8a39f4d8a2e2dc61e6da"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(f10cfc6dd4fd8c29187aa1971ff2fd87)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op f10cfc6dd4fd8c29187aa1971ff2fd87 &

# omp_op(f10cfc6dd4fd8c29187aa1971ff2fd87)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op f10cfc6dd4fd8c29187aa1971ff2fd87 &
wait

