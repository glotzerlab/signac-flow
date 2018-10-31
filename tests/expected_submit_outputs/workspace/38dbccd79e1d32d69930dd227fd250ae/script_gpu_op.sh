#PBS -N SubmissionTe/38dbccd7/gpu_op/0000/5895be08701bd70d1101b231025b67f1
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(38dbccd79e1d32d69930dd227fd250ae)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 38dbccd79e1d32d69930dd227fd250ae

