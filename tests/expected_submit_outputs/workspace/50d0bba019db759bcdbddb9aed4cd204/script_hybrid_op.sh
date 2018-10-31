#PBS -N SubmissionTe/50d0bba0/hybrid_op/0000/846fcd043100c663bddfde73b5f3ec58
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(50d0bba019db759bcdbddb9aed4cd204)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 50d0bba019db759bcdbddb9aed4cd204

