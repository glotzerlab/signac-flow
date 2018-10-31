#PBS -N SubmissionTe/5c02d3da/hybrid_op/0000/845ea4e43e6cabf674351c622af924a5
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(5c02d3da683f5a590dc86cc4821c07b0)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 5c02d3da683f5a590dc86cc4821c07b0

