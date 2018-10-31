#PBS -N SubmissionTe/40c5efdd/mpi_op/0000/c75e8029b31e40240f0f86a3e13d6f2a
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(40c5efdd19a4c2f25070b7b9336d7249)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 40c5efdd19a4c2f25070b7b9336d7249

