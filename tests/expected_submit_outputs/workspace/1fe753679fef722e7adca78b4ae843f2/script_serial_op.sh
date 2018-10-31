#!/bin/bash
#SBATCH --job-name="SubmissionTe/1fe75367/serial_op/0000/62aef239521f0cfb2084165afabc624b"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(1fe753679fef722e7adca78b4ae843f2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 1fe753679fef722e7adca78b4ae843f2

