#!/bin/bash
#SBATCH --nodes=8
#SBATCH --time=0:02:30
#SBATCH --partition=workstations

echo "nodes, systime" > node_results.csv
for i in {1..8}
do
    /usr/bin/time --format "$i, %E" srun --partition=workstations --nodes=$i --ntasks-per-node=1 python3 ./assignment3.py -n 500000000 &>> node_results.csv
done
