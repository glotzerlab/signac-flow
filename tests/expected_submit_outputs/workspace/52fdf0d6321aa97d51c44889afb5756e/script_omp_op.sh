#PBS -N SubmissionTe/52fdf0d6/omp_op/0000/d9ac9bdd29f061f1d2743db9642b81f5
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(52fdf0d6321aa97d51c44889afb5756e)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 52fdf0d6321aa97d51c44889afb5756e

