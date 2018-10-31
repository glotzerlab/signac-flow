#!/bin/bash
#SBATCH --job-name="SubmissionTest/bundle/5d762c9d696342037d4fca7721c03a979abdd628"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(0a53cc674a835b8a34a187dece361f03)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 0a53cc674a835b8a34a187dece361f03

# omp_op(0a53cc674a835b8a34a187dece361f03)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 0a53cc674a835b8a34a187dece361f03

