#PBS -N SubmissionTe/cf8cad4e/hybrid_op/0000/2b0755cc006a5fa3343e4521ea87af8c
#PBS -V
#PBS -l nodes=4
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(cf8cad4e97c9e890fd45dcaeb6f4e885)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op cf8cad4e97c9e890fd45dcaeb6f4e885

