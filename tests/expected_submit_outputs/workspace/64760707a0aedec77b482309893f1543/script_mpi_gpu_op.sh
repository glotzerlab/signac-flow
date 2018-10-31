#PBS -N SubmissionTe/64760707/mpi_gpu_op/0000/aac46072256b7168724ebc7b4f6d1098
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(64760707a0aedec77b482309893f1543)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 64760707a0aedec77b482309893f1543

