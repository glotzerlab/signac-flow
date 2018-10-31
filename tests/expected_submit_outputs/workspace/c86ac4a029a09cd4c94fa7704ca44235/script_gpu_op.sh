#PBS -N SubmissionTe/c86ac4a0/gpu_op/0000/879981d910981972056b573294f30cd2
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(c86ac4a029a09cd4c94fa7704ca44235)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op c86ac4a029a09cd4c94fa7704ca44235

