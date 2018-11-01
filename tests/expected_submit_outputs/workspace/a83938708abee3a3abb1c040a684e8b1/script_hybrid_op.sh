#SBATCH --job-name="SubmissionTe/a8393870/hybrid_op/0000
#SBATCH --partition=RM
#SBATCH -t 01:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node 4
export OMP_NUM_THREADS=2
