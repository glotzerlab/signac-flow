#PBS -N SubmissionTe/40c5efdd/mpi_gpu_op/0000/7371b5a44f9cceac57333d0eb69a4781
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(40c5efdd19a4c2f25070b7b9336d7249)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 40c5efdd19a4c2f25070b7b9336d7249

