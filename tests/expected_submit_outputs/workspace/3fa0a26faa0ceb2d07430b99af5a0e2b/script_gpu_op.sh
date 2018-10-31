#PBS -N SubmissionTe/3fa0a26f/gpu_op/0000/5d43a69062d8547687319d45000d17d7
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(3fa0a26faa0ceb2d07430b99af5a0e2b)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 3fa0a26faa0ceb2d07430b99af5a0e2b

