#PBS -N SubmissionTe/ebfc54f9/gpu_op/0000/039acd92f1f9b68ea3f3b664d8fcd457
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(ebfc54f91234046b181feb05e4bf56a2)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op ebfc54f91234046b181feb05e4bf56a2 &
wait

