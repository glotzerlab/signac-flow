#PBS -N SubmissionTest/bundle/3cc6a2d2e83b9e7a3b86bfe441ce0a80d897e1c5
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(636b26ebd5ec9548239c297990e1fe11)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 636b26ebd5ec9548239c297990e1fe11

# omp_op(636b26ebd5ec9548239c297990e1fe11)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 636b26ebd5ec9548239c297990e1fe11

