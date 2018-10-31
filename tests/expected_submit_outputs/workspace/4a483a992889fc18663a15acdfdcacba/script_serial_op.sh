#!/bin/bash
#SBATCH --job-name="SubmissionTe/4a483a99/serial_op/0000/7542f4350b7bb3bf6c2578125cddf06b"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(4a483a992889fc18663a15acdfdcacba)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 4a483a992889fc18663a15acdfdcacba

