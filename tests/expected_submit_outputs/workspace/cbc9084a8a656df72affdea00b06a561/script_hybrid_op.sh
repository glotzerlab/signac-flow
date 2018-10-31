#PBS -N SubmissionTe/cbc9084a/hybrid_op/0000/834d3107058baabe8de57bd74dc2005c
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(cbc9084a8a656df72affdea00b06a561)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op cbc9084a8a656df72affdea00b06a561 &
wait

