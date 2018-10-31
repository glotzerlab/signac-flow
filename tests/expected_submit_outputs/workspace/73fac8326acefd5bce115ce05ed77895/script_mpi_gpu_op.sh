#PBS -N SubmissionTe/73fac832/mpi_gpu_op/0000/3bf65a3bc64877e5c8c0b2250c19de3a
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(73fac8326acefd5bce115ce05ed77895)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 73fac8326acefd5bce115ce05ed77895

