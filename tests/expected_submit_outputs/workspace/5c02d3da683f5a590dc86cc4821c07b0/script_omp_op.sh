#PBS -N SubmissionTe/5c02d3da/omp_op/0000/64453bed559375d8001ae362f7ea729d
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(5c02d3da683f5a590dc86cc4821c07b0)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 5c02d3da683f5a590dc86cc4821c07b0

