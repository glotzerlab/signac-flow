#PBS -N SubmissionTe/ebfc54f9/omp_op/0000/64e3c40f2410746a57556beaeda92cb3
#PBS -V
#PBS -l nodes=2
#PBS -l pmem=
#PBS -l qos=flux
#PBS -q flux

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# omp_op(ebfc54f91234046b181feb05e4bf56a2)
export OMP_NUM_THREADS=2
/Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec omp_op ebfc54f91234046b181feb05e4bf56a2 &
wait

