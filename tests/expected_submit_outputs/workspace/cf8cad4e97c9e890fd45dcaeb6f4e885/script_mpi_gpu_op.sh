#PBS -N SubmissionTe/cf8cad4e/mpi_gpu_op/0000/d5806e2dff4807f6e627a2e1c0d9f7ce
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(cf8cad4e97c9e890fd45dcaeb6f4e885)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op cf8cad4e97c9e890fd45dcaeb6f4e885

