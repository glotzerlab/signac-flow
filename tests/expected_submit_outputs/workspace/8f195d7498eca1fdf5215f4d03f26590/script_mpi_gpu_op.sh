#PBS -N SubmissionTe/8f195d74/mpi_gpu_op/0000/296b7551bfe42783ee0664c826aa8f4f
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(8f195d7498eca1fdf5215f4d03f26590)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 8f195d7498eca1fdf5215f4d03f26590

