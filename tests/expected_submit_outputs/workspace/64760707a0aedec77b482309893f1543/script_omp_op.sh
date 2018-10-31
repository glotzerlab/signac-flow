#PBS -N SubmissionTe/64760707/omp_op/0000/91214b8dde1f2cceabb275c7219841f8
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(64760707a0aedec77b482309893f1543)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 64760707a0aedec77b482309893f1543

