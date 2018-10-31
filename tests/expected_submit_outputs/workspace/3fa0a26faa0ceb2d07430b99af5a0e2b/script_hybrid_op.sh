#PBS -N SubmissionTe/3fa0a26f/hybrid_op/0000/fd21b3a453a89d93807d1c1a87d394dc
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(3fa0a26faa0ceb2d07430b99af5a0e2b)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 3fa0a26faa0ceb2d07430b99af5a0e2b

