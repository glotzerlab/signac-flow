#PBS -N SubmissionTe/52fdf0d6/serial_op/0000/ef92fb9b2281060d1aaf10e995ecbfb7
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(52fdf0d6321aa97d51c44889afb5756e)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 52fdf0d6321aa97d51c44889afb5756e

