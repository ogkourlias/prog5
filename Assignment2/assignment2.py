#!/usr/bin/env python3

"""
    usage:
"""

# METADATA VARIABLES
__author__ = "Orfeas Gkourlias"
__status__ = "WIP"
__version__ = "0.1"

# IMPORTS
import sys
import argparse
import numpy as np
import math
from mpi4py import MPI

# FUNCTIONS
def argparser():
    """Argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--lower", "-a", required=True, type=int)
    parser.add_argument("--upper", "-b", required=True, type=int)
    parser.add_argument("--steps", "-n", default=2, type=int)
    return parser.parse_args()

def trapezoid(a, b, n):
    """I = trapezoid(f, a, b, *, n=...).
    Calculates the definite integral of the function f(x)
    from a to b using the composite trapezoidal rule with
    n subdivisions (with default n=...).
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    if rank == 0:
        work_dict = {}
        linspace = np.linspace(a, b, n)
        chunk = int(n/size)
        start = 0
        for i in range(1,size):
            work_dict[i] = linspace[start:start+chunk]
            start += chunk
        work_dict[size] = linspace[start:]
    else:
        work_dict = None
    work_dict = comm.bcast(work_dict, root=0)
    # print(work_dict[rank])
    print(work_dict[rank+1][0], work_dict[rank+1][-1])

def trapezoid_2(a, b, n):
    """I = trapezoid(f, a, b, *, n=...).
    Calculates the definite integral of the function f(x)
    from a to b using the composite trapezoidal rule with
    n subdivisions (with default n=...).
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    chunk = int(math.ceil(n/(size-1)))
    local_integral = 0
    if rank == 0:
        linspace = np.linspace(a, b, n)
    else:
        linspace = None
    linspace = comm.bcast(linspace, root=0)
    start = chunk*rank
    end = start + chunk
    work = np.array(linspace[start:end])
    if start == 0 or end-1 == len(linspace):
        local_integral += np.cos(work[0]) + np.cos(work[-1])
        local_integral += np.sum(np.cos(work[1:-2]))
    else:
        local_integral += np.sum(np.cos(work))

    results = comm.gather(local_integral, root=0)

    if rank == 0:
        integral = sum(results)
        integral *= (b-a) / n
        exact = np.sin(b) - np.sin(a)
        print(integral, exact)
        return integral
# MAIN
def main(args):
    """ Main function """
    args = argparser()
    trapezoid_2(args.lower, args.upper, args.steps)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
