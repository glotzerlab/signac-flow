#PBS -N SubmissionTe/118f4f3c/mpi_op/0000/f7cbad5abe0b9b67bc644aade4d604b7
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(118f4f3ccfae13a187ade1247f33dd3c)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 118f4f3ccfae13a187ade1247f33dd3c

