#PBS -N SubmissionTe/a43b5a3b/serial_op/0000/b9795adfb673cf521d3c406bbb48534f
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(a43b5a3b12e3499d4e23ba6f7ad011b1)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op a43b5a3b12e3499d4e23ba6f7ad011b1

