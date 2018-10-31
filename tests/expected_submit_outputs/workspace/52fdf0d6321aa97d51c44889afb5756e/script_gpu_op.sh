#PBS -N SubmissionTe/52fdf0d6/gpu_op/0000/f52659daa760cdbf928b1964e4ab8941
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(52fdf0d6321aa97d51c44889afb5756e)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 52fdf0d6321aa97d51c44889afb5756e

