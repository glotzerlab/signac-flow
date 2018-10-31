#PBS -N SubmissionTe/40c5efdd/serial_op/0000/7c8cfb1b7abc1824eeb2e8c6bdb46e44
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(40c5efdd19a4c2f25070b7b9336d7249)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 40c5efdd19a4c2f25070b7b9336d7249

