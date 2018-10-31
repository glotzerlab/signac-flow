#!/bin/bash
#SBATCH --job-name="SubmissionTe/37545281/parallel_op/0000/9115001f8d4e7829f33fadce56a2dfe2"
#SBATCH --partition=skx-normal
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(37545281479eb2ac074fef1d280ae0bd)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 37545281479eb2ac074fef1d280ae0bd

