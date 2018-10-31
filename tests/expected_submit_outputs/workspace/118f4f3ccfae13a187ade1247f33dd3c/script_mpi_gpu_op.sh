#PBS -N SubmissionTe/118f4f3c/mpi_gpu_op/0000/824737a29b4b45a29fd09506fbadd1a5
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(118f4f3ccfae13a187ade1247f33dd3c)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 118f4f3ccfae13a187ade1247f33dd3c

