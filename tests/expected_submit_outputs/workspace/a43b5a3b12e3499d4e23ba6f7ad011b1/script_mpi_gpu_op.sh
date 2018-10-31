#PBS -N SubmissionTe/a43b5a3b/mpi_gpu_op/0000/a2360313b62507438a20169bdd22d025
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(a43b5a3b12e3499d4e23ba6f7ad011b1)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op a43b5a3b12e3499d4e23ba6f7ad011b1

