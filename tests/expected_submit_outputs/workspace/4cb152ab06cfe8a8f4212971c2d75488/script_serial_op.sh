#PBS -N SubmissionTe/4cb152ab/serial_op/0000/89ef30808834640d948089048c707c89
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(4cb152ab06cfe8a8f4212971c2d75488)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 4cb152ab06cfe8a8f4212971c2d75488

