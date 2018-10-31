#PBS -N SubmissionTe/c86ac4a0/mpi_op/0000/ebfb350273eeb6dfc3f6d6000a8203df
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(c86ac4a029a09cd4c94fa7704ca44235)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op c86ac4a029a09cd4c94fa7704ca44235

