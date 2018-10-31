#PBS -N SubmissionTe/cbc9084a/omp_op/0000/2734c0a58a93490d8a4527b036010b15
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(cbc9084a8a656df72affdea00b06a561)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op cbc9084a8a656df72affdea00b06a561 &
wait

