#!/bin/bash
#SBATCH --job-name="SubmissionTest/bundle/cdb9b1d98285204cbead1b289909fa2ba665086f"
#SBATCH --partition=RM
#SBATCH -N 1
#SBATCH --ntasks-per-node 2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(5bea7f4064798af873fad258b21cf034)
mpirun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 5bea7f4064798af873fad258b21cf034

# omp_op(5bea7f4064798af873fad258b21cf034)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 5bea7f4064798af873fad258b21cf034

