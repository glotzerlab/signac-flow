#PBS -N SubmissionTest/bundle/1ec98b318155c5db8e614ea747d8a7ae75668d2c
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(b293e1851c5881828462bb7a06dce02c)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op b293e1851c5881828462bb7a06dce02c

# omp_op(b293e1851c5881828462bb7a06dce02c)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op b293e1851c5881828462bb7a06dce02c

