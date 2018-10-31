#PBS -N SubmissionTe/fd5f1576/hybrid_op/0000/7d7068b8b618002beab00812084e6ee4
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(fd5f15761bc3017ad4069d08ab9dc3d2)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op fd5f15761bc3017ad4069d08ab9dc3d2 &
wait

