#PBS -N SubmissionTe/cf8cad4e/gpu_op/0000/74e9bebe5335666f4fbb57d8fdfad6d8
#PBS -V
#PBS -l nodes=1
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# gpu_op(cf8cad4e97c9e890fd45dcaeb6f4e885)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec gpu_op cf8cad4e97c9e890fd45dcaeb6f4e885

