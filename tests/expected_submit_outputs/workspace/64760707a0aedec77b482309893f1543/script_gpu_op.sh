#PBS -N SubmissionTe/64760707/gpu_op/0000/86c570560b72593ede451b21e1e97607
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(64760707a0aedec77b482309893f1543)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 64760707a0aedec77b482309893f1543

