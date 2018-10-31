#PBS -N SubmissionTe/fd5f1576/serial_op/0000/4708823c0f4db2a0baa513c2c7c81c84
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(fd5f15761bc3017ad4069d08ab9dc3d2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op fd5f15761bc3017ad4069d08ab9dc3d2 &
wait

