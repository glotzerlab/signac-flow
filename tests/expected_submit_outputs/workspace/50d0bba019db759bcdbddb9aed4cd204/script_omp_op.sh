#PBS -N SubmissionTe/50d0bba0/omp_op/0000/ab931a862818c7a6b96b7f79abdcdaf3
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(50d0bba019db759bcdbddb9aed4cd204)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 50d0bba019db759bcdbddb9aed4cd204

