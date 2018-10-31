#PBS -N SubmissionTe/5c02d3da/mpi_op/0000/e9b9db9cc8e0eb6fc8f302e2be28b498
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(5c02d3da683f5a590dc86cc4821c07b0)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 5c02d3da683f5a590dc86cc4821c07b0

