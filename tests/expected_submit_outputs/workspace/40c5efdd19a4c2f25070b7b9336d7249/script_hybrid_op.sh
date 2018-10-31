#PBS -N SubmissionTe/40c5efdd/hybrid_op/0000/71558e1f1cac3bd81081f4e79177a19b
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(40c5efdd19a4c2f25070b7b9336d7249)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 40c5efdd19a4c2f25070b7b9336d7249

