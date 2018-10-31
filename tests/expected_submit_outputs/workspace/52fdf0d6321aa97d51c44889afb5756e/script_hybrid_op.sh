#PBS -N SubmissionTe/52fdf0d6/hybrid_op/0000/3ae69f7004d90b93a7b3b3dbeb219ae0
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=4
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(52fdf0d6321aa97d51c44889afb5756e)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 52fdf0d6321aa97d51c44889afb5756e

