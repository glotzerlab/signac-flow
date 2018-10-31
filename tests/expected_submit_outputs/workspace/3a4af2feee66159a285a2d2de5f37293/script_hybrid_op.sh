#PBS -N SubmissionTe/3a4af2fe/hybrid_op/0000/792855c81c7041eb0d6adcb9a008212f
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(3a4af2feee66159a285a2d2de5f37293)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 3a4af2feee66159a285a2d2de5f37293

