#!/bin/bash
#SBATCH --job-name="SubmissionTe/4a483a99/hybrid_op/0000/60f16688ac95e11c7b55b8bad2bdcf91"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(4a483a992889fc18663a15acdfdcacba)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 4a483a992889fc18663a15acdfdcacba

