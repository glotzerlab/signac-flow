#PBS -N SubmissionTest/bundle/f18e5572493ddf0fcfd027a8564f1ebf161cd706
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(def67f27e79966e068d16dcf943a7921)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op def67f27e79966e068d16dcf943a7921 &

# omp_op(def67f27e79966e068d16dcf943a7921)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op def67f27e79966e068d16dcf943a7921 &
wait

