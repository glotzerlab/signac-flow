#PBS -N SubmissionTe/fd5f1576/gpu_op/0000/b3fbf84d43c4361d9110cb6fa7fa53a3
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(fd5f15761bc3017ad4069d08ab9dc3d2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op fd5f15761bc3017ad4069d08ab9dc3d2 &
wait

