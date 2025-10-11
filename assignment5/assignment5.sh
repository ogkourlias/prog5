#!/bin/bash
#SBATCH -c 4
#SBATCH --ntasks=4
#SBATCH --mem=16gb
#SBATCH --time=0:19:59

./assignment5.py -i /local-fs/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff
