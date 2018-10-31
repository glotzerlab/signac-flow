#PBS -N SubmissionTe/ebfc54f9/serial_op/0000/d69cec18ee527ede5c23e78bd5900ccb
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(ebfc54f91234046b181feb05e4bf56a2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op ebfc54f91234046b181feb05e4bf56a2 &
wait

