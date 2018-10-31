#PBS -N SubmissionTe/cbc9084a/mpi_gpu_op/0000/de7edb5dbb057213d6cc895e4a875261
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(cbc9084a8a656df72affdea00b06a561)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op cbc9084a8a656df72affdea00b06a561 &
wait

