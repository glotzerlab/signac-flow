#PBS -N SubmissionTe/fd5f1576/parallel_op/0000/fac98fb82f8109fc4904607b473597cd
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(fd5f15761bc3017ad4069d08ab9dc3d2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op fd5f15761bc3017ad4069d08ab9dc3d2 &
wait

