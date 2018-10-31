#PBS -N SubmissionTe/cf8cad4e/mpi_op/0000/8b5f8392c2cb99bdb76938061fb61212
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(cf8cad4e97c9e890fd45dcaeb6f4e885)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op cf8cad4e97c9e890fd45dcaeb6f4e885

