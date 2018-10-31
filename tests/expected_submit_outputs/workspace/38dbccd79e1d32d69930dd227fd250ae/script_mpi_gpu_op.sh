#PBS -N SubmissionTe/38dbccd7/mpi_gpu_op/0000/f7796fb6038f930e53a6c183b66322e1
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(38dbccd79e1d32d69930dd227fd250ae)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 38dbccd79e1d32d69930dd227fd250ae

