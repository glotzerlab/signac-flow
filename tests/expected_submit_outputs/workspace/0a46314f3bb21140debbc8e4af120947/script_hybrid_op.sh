#SBATCH --job-name="SubmissionTe/0a46314f/hybrid_op/0000
#SBATCH --partition=compute
#SBATCH -t 01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
export OMP_NUM_THREADS=2
