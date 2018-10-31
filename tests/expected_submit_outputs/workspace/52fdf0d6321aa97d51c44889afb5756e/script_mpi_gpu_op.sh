#PBS -N SubmissionTe/52fdf0d6/mpi_gpu_op/0000/de1ee93d2d3e4c7c6e8b0aa698cbf84e
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(52fdf0d6321aa97d51c44889afb5756e)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 52fdf0d6321aa97d51c44889afb5756e

