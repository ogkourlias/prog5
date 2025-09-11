#!/bin/bash
#SBATCH -c 1                      
#SBATCH --mem=512mb                   #
#SBATCH --time=0:00:30                # 30 secs

for i in {0..5}
do
    python3 ./assignment1.py -a $(($i-1)) -b $(($i+1)) -n 2000
done
