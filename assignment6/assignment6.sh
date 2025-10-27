#!/bin/bash
#SBATCH -c 1
#SBATCH --ntasks=1
#SBATCH --mem=8gb
#SBATCH --time=0:19:59

./assignment6.py -i ./test.txt
