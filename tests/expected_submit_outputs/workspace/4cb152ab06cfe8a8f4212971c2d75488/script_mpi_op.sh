#PBS -N SubmissionTe/4cb152ab/mpi_op/0000/5aa0855e5d20fe4998547670ab4395b6
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(4cb152ab06cfe8a8f4212971c2d75488)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 4cb152ab06cfe8a8f4212971c2d75488

