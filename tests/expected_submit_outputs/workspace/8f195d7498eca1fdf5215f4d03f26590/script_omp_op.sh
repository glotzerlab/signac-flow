#PBS -N SubmissionTe/8f195d74/omp_op/0000/0fbe27f9dcfbb58c11c4c8adb5b889e4
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(8f195d7498eca1fdf5215f4d03f26590)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 8f195d7498eca1fdf5215f4d03f26590

