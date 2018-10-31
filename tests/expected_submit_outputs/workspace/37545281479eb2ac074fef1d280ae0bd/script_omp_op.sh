#!/bin/bash
#SBATCH --job-name="SubmissionTe/37545281/omp_op/0000/e4ee45fa5076398d481d4f61b2ee09bd"
#SBATCH --partition=skx-normal
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(37545281479eb2ac074fef1d280ae0bd)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 37545281479eb2ac074fef1d280ae0bd

