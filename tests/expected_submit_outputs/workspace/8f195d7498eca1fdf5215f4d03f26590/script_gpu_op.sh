#PBS -N SubmissionTe/8f195d74/gpu_op/0000/926725b8f14f29f553eace2b6b7bd1f7
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(8f195d7498eca1fdf5215f4d03f26590)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op 8f195d7498eca1fdf5215f4d03f26590

