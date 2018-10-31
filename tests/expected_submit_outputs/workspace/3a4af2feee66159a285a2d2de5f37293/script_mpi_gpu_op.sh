#PBS -N SubmissionTe/3a4af2fe/mpi_gpu_op/0000/6460b1cfb1e1058b2d8520ce3b2266a9
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(3a4af2feee66159a285a2d2de5f37293)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op 3a4af2feee66159a285a2d2de5f37293

