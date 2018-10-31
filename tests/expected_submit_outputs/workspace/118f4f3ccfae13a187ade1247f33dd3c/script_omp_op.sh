#PBS -N SubmissionTe/118f4f3c/omp_op/0000/3483cdff26cb9fe03ad94673d1d135c8
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(118f4f3ccfae13a187ade1247f33dd3c)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 118f4f3ccfae13a187ade1247f33dd3c

