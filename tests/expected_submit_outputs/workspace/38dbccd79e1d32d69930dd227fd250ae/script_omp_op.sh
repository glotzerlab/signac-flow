#PBS -N SubmissionTe/38dbccd7/omp_op/0000/ab8e4737b3caa76848f3bd247015391c
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(38dbccd79e1d32d69930dd227fd250ae)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 38dbccd79e1d32d69930dd227fd250ae

