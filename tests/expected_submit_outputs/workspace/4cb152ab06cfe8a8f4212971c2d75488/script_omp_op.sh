#PBS -N SubmissionTe/4cb152ab/omp_op/0000/65b255361a4ae9f507be92196de02705
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(4cb152ab06cfe8a8f4212971c2d75488)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 4cb152ab06cfe8a8f4212971c2d75488

