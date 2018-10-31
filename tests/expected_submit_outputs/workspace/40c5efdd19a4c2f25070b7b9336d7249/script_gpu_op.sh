#PBS -N SubmissionTe/40c5efdd/gpu_op/0000/53d8b4811fc56a5f2f0c6f13729123a7
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(40c5efdd19a4c2f25070b7b9336d7249)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 40c5efdd19a4c2f25070b7b9336d7249

