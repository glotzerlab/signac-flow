#PBS -N SubmissionTe/cf8cad4e/omp_op/0000/a14090959672a9b2f8192b861e95f16b
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(cf8cad4e97c9e890fd45dcaeb6f4e885)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op cf8cad4e97c9e890fd45dcaeb6f4e885

