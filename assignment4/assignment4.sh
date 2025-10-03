#!/bin/bash
#SBATCH -c 1
#SBATCH --ntasks=1            
#SBATCH --mem=16gb
#SBATCH --time=0:09:59

./assignment4.py -i /local-fs/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff
