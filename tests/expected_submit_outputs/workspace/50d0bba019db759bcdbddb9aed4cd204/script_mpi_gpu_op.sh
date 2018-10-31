#PBS -N SubmissionTe/50d0bba0/mpi_gpu_op/0000/1e2bde142292cedcaeeccdf980e459fa
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(50d0bba019db759bcdbddb9aed4cd204)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 50d0bba019db759bcdbddb9aed4cd204

