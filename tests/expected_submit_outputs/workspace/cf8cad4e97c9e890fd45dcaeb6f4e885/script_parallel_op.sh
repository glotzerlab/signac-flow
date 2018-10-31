#PBS -N SubmissionTe/cf8cad4e/parallel_op/0000/8e7b8bc908024c4446b387a6b6f23a73
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(cf8cad4e97c9e890fd45dcaeb6f4e885)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op cf8cad4e97c9e890fd45dcaeb6f4e885

