#PBS -N SubmissionTe/c86ac4a0/hybrid_op/0000/ae2ca541eae930bc0a04988b7d618157
#PBS -V
#PBS -l nodes=4
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(c86ac4a029a09cd4c94fa7704ca44235)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op c86ac4a029a09cd4c94fa7704ca44235

