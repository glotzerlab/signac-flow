#PBS -N SubmissionTe/a43b5a3b/omp_op/0000/21845070af93f59419ea9f6baf018bb1
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(a43b5a3b12e3499d4e23ba6f7ad011b1)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op a43b5a3b12e3499d4e23ba6f7ad011b1

