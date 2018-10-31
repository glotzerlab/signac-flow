#PBS -N SubmissionTe/52fdf0d6/parallel_op/0000/0df6933dd6fe869bd4334bb8239719aa
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(52fdf0d6321aa97d51c44889afb5756e)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 52fdf0d6321aa97d51c44889afb5756e

