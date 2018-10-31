#PBS -N SubmissionTe/a43b5a3b/mpi_op/0000/4d38f984f04146eebfa6be51f6eea767
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(a43b5a3b12e3499d4e23ba6f7ad011b1)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op a43b5a3b12e3499d4e23ba6f7ad011b1

