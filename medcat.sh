#!/bin/bash -l
#SBATCH --job-name=med51
#SBATCH --partition=brc,shared
#SBATCH --output=output.array.%A.%a
#SBATCH --array=21,26,39,61,91,95,176
#SBATCH -n 5
#SBATCH --time=0-1:30 
#SBATCH --mem=25480

conda activate medcat 
# pip install scispacy_models/en_core_sci_scibert-0.4.0.tar.gz

python medcat_ann_job.py $SLURM_ARRAY_TASK_ID 