#PBS -N SubmissionTe/8f195d74/serial_op/0000/d43cbc08e3b4c2f28aeaa06cfdcf7b4a
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# serial_op(8f195d7498eca1fdf5215f4d03f26590)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec serial_op 8f195d7498eca1fdf5215f4d03f26590

