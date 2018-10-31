#PBS -N SubmissionTe/c6eca32b/hybrid_op/0000/01f67a28ba60569752912bfe2175a532
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(c6eca32bee7206857127ee07e4bb8a4e)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op c6eca32bee7206857127ee07e4bb8a4e

