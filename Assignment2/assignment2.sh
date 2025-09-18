#!/bin/bash
#SBATCH -c 1                      
#SBATCH --mem=2048mb                   #
#SBATCH --time=0:00:30                # 30 secs

mpiexec -n 32 python3 ./assignment2.py -a -1 -b 1 -n 500000
