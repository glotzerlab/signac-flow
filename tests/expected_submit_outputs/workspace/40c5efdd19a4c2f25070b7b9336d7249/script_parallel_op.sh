#PBS -N SubmissionTe/40c5efdd/parallel_op/0000/5cd8ff2df397b3167216ca1562c095ff
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(40c5efdd19a4c2f25070b7b9336d7249)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 40c5efdd19a4c2f25070b7b9336d7249

