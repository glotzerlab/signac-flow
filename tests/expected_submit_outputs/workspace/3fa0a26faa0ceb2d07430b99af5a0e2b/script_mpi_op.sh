#PBS -N SubmissionTe/3fa0a26f/mpi_op/0000/692327a01700ea83edd6b0c351a1f977
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(3fa0a26faa0ceb2d07430b99af5a0e2b)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 3fa0a26faa0ceb2d07430b99af5a0e2b

