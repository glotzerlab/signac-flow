#PBS -N SubmissionTe/8f195d74/hybrid_op/0000/016e6f4a1403c8e3c0c793127f254fd1
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(8f195d7498eca1fdf5215f4d03f26590)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 8f195d7498eca1fdf5215f4d03f26590

