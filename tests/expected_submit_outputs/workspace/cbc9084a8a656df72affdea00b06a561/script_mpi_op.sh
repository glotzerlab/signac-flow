#PBS -N SubmissionTe/cbc9084a/mpi_op/0000/8288665bc12297a0accf8f02a4fe7f59
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(cbc9084a8a656df72affdea00b06a561)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op cbc9084a8a656df72affdea00b06a561 &
wait

