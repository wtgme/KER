#!/bin/bash -l
#SBATCH --job-name=sci51
#SBATCH --partition=brc,shared
#SBATCH --output=output.array.%A.%a
#SBATCH --array=113-1000
#SBATCH -n 5
#SBATCH --time=0-1:00 
#SBATCH --mem=20480

conda activate py3 
# pip install scispacy_models/en_core_sci_scibert-0.4.0.tar.gz

python scispacy_ann_job.py $SLURM_ARRAY_TASK_ID 