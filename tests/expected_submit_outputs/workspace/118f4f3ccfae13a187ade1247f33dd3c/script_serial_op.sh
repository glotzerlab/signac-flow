#PBS -N SubmissionTe/118f4f3c/serial_op/0000/7bff74417cc7cc96704071b04fdcde2b
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(118f4f3ccfae13a187ade1247f33dd3c)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 118f4f3ccfae13a187ade1247f33dd3c

