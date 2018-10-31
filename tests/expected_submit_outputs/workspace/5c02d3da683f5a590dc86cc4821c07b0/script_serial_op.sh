#PBS -N SubmissionTe/5c02d3da/serial_op/0000/d095290c582534b8a8dfc5f27b06df0b
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(5c02d3da683f5a590dc86cc4821c07b0)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 5c02d3da683f5a590dc86cc4821c07b0

