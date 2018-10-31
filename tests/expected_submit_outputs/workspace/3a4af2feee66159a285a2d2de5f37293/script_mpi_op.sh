#PBS -N SubmissionTe/3a4af2fe/mpi_op/0000/e0f77f9f24f37a068a284c9f2b79f336
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(3a4af2feee66159a285a2d2de5f37293)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 3a4af2feee66159a285a2d2de5f37293

