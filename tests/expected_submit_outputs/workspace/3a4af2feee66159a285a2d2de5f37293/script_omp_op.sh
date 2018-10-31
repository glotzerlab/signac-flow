#PBS -N SubmissionTe/3a4af2fe/omp_op/0000/317d9bb8587d9a437402a2982f7bc87f
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(3a4af2feee66159a285a2d2de5f37293)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 3a4af2feee66159a285a2d2de5f37293

