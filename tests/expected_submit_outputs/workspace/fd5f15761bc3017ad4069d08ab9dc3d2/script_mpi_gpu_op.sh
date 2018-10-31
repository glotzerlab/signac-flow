#PBS -N SubmissionTe/fd5f1576/mpi_gpu_op/0000/935645e97c70a89164f54dd9665b33aa
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(fd5f15761bc3017ad4069d08ab9dc3d2)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op fd5f15761bc3017ad4069d08ab9dc3d2 &
wait

