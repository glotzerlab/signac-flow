#PBS -N SubmissionTe/ebfc54f9/mpi_op/0000/feb9a726a7436155a03bbe4403ad216c
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(ebfc54f91234046b181feb05e4bf56a2)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op ebfc54f91234046b181feb05e4bf56a2 &
wait

