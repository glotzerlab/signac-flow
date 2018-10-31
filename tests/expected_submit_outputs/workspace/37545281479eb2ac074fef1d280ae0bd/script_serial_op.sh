#!/bin/bash
#SBATCH --job-name="SubmissionTe/37545281/serial_op/0000/0a0ac38c9697b28426a2b54eadf725b6"
#SBATCH --partition=skx-normal
#SBATCH --nodes=2
#SBATCH --ntasks=1
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(37545281479eb2ac074fef1d280ae0bd)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 37545281479eb2ac074fef1d280ae0bd

