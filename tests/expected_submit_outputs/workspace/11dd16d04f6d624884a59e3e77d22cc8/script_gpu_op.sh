#PBS -N SubmissionTe/11dd16d0/gpu_op/0000/1ac4d8a0d68135fa37a8dae61c039229
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(11dd16d04f6d624884a59e3e77d22cc8)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 11dd16d04f6d624884a59e3e77d22cc8

