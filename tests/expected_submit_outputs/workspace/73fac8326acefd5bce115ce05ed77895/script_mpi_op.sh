#PBS -N SubmissionTe/73fac832/mpi_op/0000/dd74648f8cd6f62e312ff32967006a9b
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(73fac8326acefd5bce115ce05ed77895)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 73fac8326acefd5bce115ce05ed77895

