#!/bin/bash
#SBATCH --job-name="SubmissionTest/bundle/e10d6f0ca293ed056d7dd00de80a924fd332703f"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(046cd669a3d5d74abbba2662c64a1e0c)
ibrun -n 2 -o 0 task_affinity /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 046cd669a3d5d74abbba2662c64a1e0c &

# omp_op(046cd669a3d5d74abbba2662c64a1e0c)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 046cd669a3d5d74abbba2662c64a1e0c &
wait

