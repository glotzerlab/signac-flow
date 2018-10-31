#PBS -N SubmissionTe/40c5efdd/omp_op/0000/345f2dd8098cedf3862588d23229ad11
#PBS -l walltime=01:00:00
#PBS -V
#PBS -l nodes=1

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(40c5efdd19a4c2f25070b7b9336d7249)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op 40c5efdd19a4c2f25070b7b9336d7249

