#!/bin/bash
#SBATCH --job-name="SubmissionTe/6d58262f/hybrid_op/0000/cb0653a1aae0bfb2e150ac69fb801061"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

set -e
set -u

cd /Users/vramasub/local/signac-flow/tests/expected_submit_outputs

# hybrid_op(6d58262fdce7b22dd1c34bcb0e80971a)
export OMP_NUM_THREADS=2
ibrun -n 2 /Users/vramasub/miniconda3/envs/main/bin/python /Users/vramasub/local/signac-flow/tests/expected_submit_outputs/project.py exec hybrid_op 6d58262fdce7b22dd1c34bcb0e80971a &
wait

