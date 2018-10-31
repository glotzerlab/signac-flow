#PBS -N SubmissionTe/11dd16d0/serial_op/0000/25e840c342964e653f524655189f7a52
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(11dd16d04f6d624884a59e3e77d22cc8)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 11dd16d04f6d624884a59e3e77d22cc8

