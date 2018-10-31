#PBS -N SubmissionTest/bundle/41e1e141a708313321f80677d54dfc060cca564e
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(97fb213cec4e13f4044d47f5df1e217d)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 97fb213cec4e13f4044d47f5df1e217d

# omp_op(97fb213cec4e13f4044d47f5df1e217d)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 97fb213cec4e13f4044d47f5df1e217d

