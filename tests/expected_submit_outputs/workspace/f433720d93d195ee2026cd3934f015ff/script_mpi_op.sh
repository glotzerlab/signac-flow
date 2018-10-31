#!/bin/bash
#SBATCH --job-name="SubmissionTe/f433720d/mpi_op/0000/58b4847f1801122fb5d66980b70d0f6f"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(f433720d93d195ee2026cd3934f015ff)
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op f433720d93d195ee2026cd3934f015ff

