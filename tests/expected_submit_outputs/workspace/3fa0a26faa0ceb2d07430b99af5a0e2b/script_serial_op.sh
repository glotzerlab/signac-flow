#PBS -N SubmissionTe/3fa0a26f/serial_op/0000/10cb2c428de0a040168b97bf37cdfb8f
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(3fa0a26faa0ceb2d07430b99af5a0e2b)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 3fa0a26faa0ceb2d07430b99af5a0e2b

