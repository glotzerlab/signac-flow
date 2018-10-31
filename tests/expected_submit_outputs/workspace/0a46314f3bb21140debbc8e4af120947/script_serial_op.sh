#!/bin/bash
#SBATCH --job-name="SubmissionTe/0a46314f/serial_op/0000/9867b082b6cd28223ebe5dfcab787f87"
#SBATCH --partition=compute
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(0a46314f3bb21140debbc8e4af120947)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 0a46314f3bb21140debbc8e4af120947

