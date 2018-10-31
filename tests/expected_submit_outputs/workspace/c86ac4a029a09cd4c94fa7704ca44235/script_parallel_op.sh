#PBS -N SubmissionTe/c86ac4a0/parallel_op/0000/6a9eef5d83c63c5674b95c842a57be90
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(c86ac4a029a09cd4c94fa7704ca44235)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op c86ac4a029a09cd4c94fa7704ca44235

