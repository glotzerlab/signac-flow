#PBS -N SubmissionTe/4cb152ab/gpu_op/0000/065f450ab097757c884208666bdfb5d9
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(4cb152ab06cfe8a8f4212971c2d75488)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 4cb152ab06cfe8a8f4212971c2d75488

