#PBS -N SubmissionTe/ebfc54f9/mpi_gpu_op/0000/d56cc6fa12668a25defe1024b805eb8b
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_gpu_op(ebfc54f91234046b181feb05e4bf56a2)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_gpu_op ebfc54f91234046b181feb05e4bf56a2 &
wait

