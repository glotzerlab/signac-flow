#!/bin/bash
#SBATCH --job-name="SubmissionTe/37545281/hybrid_op/0000/a9479067168b0d3bdbd279b094759f54"
#SBATCH --partition=skx-normal
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(37545281479eb2ac074fef1d280ae0bd)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 37545281479eb2ac074fef1d280ae0bd

