#PBS -N SubmissionTe/ebfc54f9/hybrid_op/0000/86ee60ea38dd68d060a16ef7d76739f5
#PBS -V
#PBS -l nodes=4
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(ebfc54f91234046b181feb05e4bf56a2)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op ebfc54f91234046b181feb05e4bf56a2 &
wait

