#PBS -N SubmissionTest/bundle/d0cf4fc4163e4e23edc8879607138228921c8207
#PBS -V
#PBS -l nodes=4
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# mpi_op(4337ec17629a91d60dd5750b224ebedc)
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec mpi_op 4337ec17629a91d60dd5750b224ebedc &

# omp_op(4337ec17629a91d60dd5750b224ebedc)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 4337ec17629a91d60dd5750b224ebedc &
wait

