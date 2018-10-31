#PBS -N SubmissionTest/bundle/adeea8aca8637aec5bc5cf3d25995d5143463fe6
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(29110bb20bd8a85768c9201d72139c85)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 29110bb20bd8a85768c9201d72139c85 &

# omp_op(29110bb20bd8a85768c9201d72139c85)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 29110bb20bd8a85768c9201d72139c85 &
wait

