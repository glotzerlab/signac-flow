#PBS -N SubmissionTe/fd5f1576/omp_op/0000/e499f6a904ec72d156b83c8fa01a134c
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(fd5f15761bc3017ad4069d08ab9dc3d2)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op fd5f15761bc3017ad4069d08ab9dc3d2 &
wait

