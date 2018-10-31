#PBS -N SubmissionTe/cf8cad4e/serial_op/0000/5a8eacbf14a3b1d228352b7b0e2a7dd2
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(cf8cad4e97c9e890fd45dcaeb6f4e885)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op cf8cad4e97c9e890fd45dcaeb6f4e885

