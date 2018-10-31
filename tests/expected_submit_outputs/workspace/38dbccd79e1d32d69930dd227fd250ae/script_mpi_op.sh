#PBS -N SubmissionTe/38dbccd7/mpi_op/0000/401e003b09e6a53b6fd02fd79162630b
#PBS -V
#PBS -l nodes=2

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(38dbccd79e1d32d69930dd227fd250ae)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 38dbccd79e1d32d69930dd227fd250ae

