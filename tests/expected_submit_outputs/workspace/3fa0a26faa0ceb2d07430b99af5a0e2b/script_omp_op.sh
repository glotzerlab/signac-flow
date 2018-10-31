#PBS -N SubmissionTe/3fa0a26f/omp_op/0000/b3219844e53244d69f7bb38436cc9b3a
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(3fa0a26faa0ceb2d07430b99af5a0e2b)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 3fa0a26faa0ceb2d07430b99af5a0e2b

