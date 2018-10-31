#PBS -N SubmissionTe/c6eca32b/omp_op/0000/8dad1bd1027bc7d6348b220c22f26f17
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(c6eca32bee7206857127ee07e4bb8a4e)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op c6eca32bee7206857127ee07e4bb8a4e

