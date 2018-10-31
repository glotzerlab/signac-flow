#PBS -N SubmissionTe/ebfc54f9/parallel_op/0000/56d6c013420d8737d89dacb80b9e58f3
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(ebfc54f91234046b181feb05e4bf56a2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op ebfc54f91234046b181feb05e4bf56a2 &
wait

