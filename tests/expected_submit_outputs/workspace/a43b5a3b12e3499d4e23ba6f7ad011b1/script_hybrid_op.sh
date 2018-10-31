#PBS -N SubmissionTe/a43b5a3b/hybrid_op/0000/224e1af103c0cf7be283b4d29707daf3
#PBS -V
#PBS -l nodes=4
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(a43b5a3b12e3499d4e23ba6f7ad011b1)
export OMP_NUM_THREADS=2
mpiexec -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op a43b5a3b12e3499d4e23ba6f7ad011b1

