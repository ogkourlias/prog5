#!/usr/bin/env python3

"""
    usage:
        python3 weewooowowowowowowowoewowoeowowo
"""

# METADATA VARIABLES
__author__ = "Orfeas Gkourlias"
__status__ = "WIP"
__version__ = "0.1"

# IMPORTS
import sys
import cProfile
import argparse
import numpy as np
from mpi4py import MPI

def argparser():
    """Argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", "-n", default=2, type=int)
    return parser.parse_args()

# FUNCTIONS
def sieve_erat(n):
    """ Desc """
    end = int(np.sqrt(n))
    bools = np.ones(n-2)
    for i in range(0, end):
        if bools[i]:
            # Parallelize here
            num = i+2
            for j in range(num**2, n, num):
                bools[j-2] = 0
    return bools

def sieve_erat_mpi(n):
    """ Desc """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    end = int(np.sqrt(n))

    if rank == 0:
        # Made things needlessly difficult by not including 0 and 1 
        # In the prior iteration.
        base = np.ones(end+1, dtype=bool)
        base[0:2] = False
        for i in range(2, end + 1):
            if base[i]:
                base[i*i:end+1:i] = False
        base_primes = np.nonzero(base)[0]
    else:
        base_primes = None

    base_primes = comm.bcast(base_primes, root=0)

    chunks = (n - end) // size
    start = end + 1 + rank * chunks
    stop = end + 1 + (rank + 1) * chunks
    if rank == size - 1:
        stop = n + 1

    seg = np.ones(stop - start, dtype=bool)
    for p in base_primes:
        s = max(p * p, ((start + p - 1) // p) * p)
        seg[s - start:stop - start:p] = False

    loc = np.nonzero(seg)[0] + start
    gathered = comm.gather(loc, root=0)

    if rank == 0:
        all_primes = np.concatenate((base_primes, np.concatenate(gathered))) if gathered else base_primes
        return all_primes
    else:
        return None



# MAIN
def main(args):
    """ Main function """
    # FINISH
    args = argparser()
    cProfile.run(f'sieve_erat_mpi({args.steps})', "sieve_profiler_output.txt")
    # sieve_erat_mpi(80000)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
