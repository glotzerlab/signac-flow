#PBS -N SubmissionTe/50d0bba0/mpi_op/0000/32ef6f2fc6760e76a2968f00bdf82c97
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(50d0bba019db759bcdbddb9aed4cd204)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 50d0bba019db759bcdbddb9aed4cd204

