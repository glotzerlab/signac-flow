#!/bin/bash
#SBATCH --job-name="SubmissionTe/1d9e6021/mpi_op/0000/8b10ec9bbd6827596c4cb4370076d3c6"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(1d9e602187eaf2adadc60b0a6f6f6a60)
ibrun -n 2 -o 0 task_affinity /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 1d9e602187eaf2adadc60b0a6f6f6a60 &
wait

