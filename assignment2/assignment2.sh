#!/bin/bash
#SBATCH -c 1   
#SBATCH --ntasks=33                   
#SBATCH --mem=8gb
#SBATCH --time=0:00:30

echo "tasks, systime" > results.csv
for i in {1..32}
do
    /usr/bin/time --format "$i, %E" mpiexec -n $i python3 ./assignment2.py -a -10 -b 10 -n 5000000 &>> results.csv
done