#PBS -N SubmissionTe/a43b5a3b/parallel_op/0000/c3a52e5da02bd6bdde69fe7d1e3122d8
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# parallel_op(a43b5a3b12e3499d4e23ba6f7ad011b1)
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec parallel_op a43b5a3b12e3499d4e23ba6f7ad011b1

