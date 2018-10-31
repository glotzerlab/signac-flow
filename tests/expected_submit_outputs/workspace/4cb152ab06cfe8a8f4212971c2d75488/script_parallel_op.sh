#PBS -N SubmissionTe/4cb152ab/parallel_op/0000/0859f007b611edd58b1c6ab608944d5e
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(4cb152ab06cfe8a8f4212971c2d75488)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op 4cb152ab06cfe8a8f4212971c2d75488

