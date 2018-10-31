#PBS -N SubmissionTe/64760707/hybrid_op/0000/ef147530711565d21483ba4981f0f215
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(64760707a0aedec77b482309893f1543)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 64760707a0aedec77b482309893f1543

