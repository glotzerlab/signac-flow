#!/bin/bash
#SBATCH --job-name="SubmissionTe/4a483a99/parallel_op/0000/11d1a4561e5004948b8a73f5b730e6c3"
#SBATCH --partition=skx-normal
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --partition=skx-normal

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(4a483a992889fc18663a15acdfdcacba)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 4a483a992889fc18663a15acdfdcacba

