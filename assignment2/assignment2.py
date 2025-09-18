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
import math
import numpy as np
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
    # MPI Init
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Dividing chunk sizes based on number of workers
    chunk = int(math.ceil(n / (size - 1))) if size > 1 else n

    # Set local for worker
    local_integral = 0

    if rank == 0:
        linspace = np.linspace(a, b, n)
    else:
        linspace = None

    # Broadcast if rank 0
    linspace = comm.bcast(linspace, root=0)

    # Calculate start, stop and work on linscape for worker
    start = chunk * rank
    end = start + chunk
    work = np.array(linspace[start:end])

    # Trapezoidal calculation
    if start == 0 or end - 1 == len(linspace):
        local_integral += np.cos(work[0]) + np.cos(work[-1])
        local_integral += np.sum(np.cos(work[1:-2]))
    else:
        local_integral += np.sum(np.cos(work))

    # Gather all local integrals on node 0
    results = comm.gather(local_integral, root=0)

    # Sum all local integrals for final integral
    if rank == 0:
        integral = sum(results)
        integral *= (b - a) / n
        exact = np.sin(b) - np.sin(a)
        return integral, exact


def trapezoid_scatter(a, b, n):
    """I = trapezoid(f, a, b, *, n=...).
    Calculates the definite integral of the function f(x)
    from a to b using the composite trapezoidal rule with
    n subdivisions (with default n=...).
    """
    # MPI Init
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Dividing chunk sizes based on number of workers
    chunk = int(math.ceil(n / (size))) if size > 1 else n

    # Set local for worker
    local_integral = 0

    if rank == 0:
        linspace = np.linspace(a, b, n)
        linspace = [linspace[i : i + chunk] for i in range(0, len(linspace), chunk)]
    else:
        linspace = None

    # Scatter if rank 0
    linspace = comm.scatter(linspace, root=0)

    # Trapezoidal calculation
    if linspace[0] == a or linspace[-1] == b:
        local_integral += np.cos(linspace[0]) + np.cos(linspace[-1])
        local_integral += np.sum(np.cos(linspace[1:-2]))
    else:
        local_integral += np.sum(np.cos(linspace))

    # Gather all local integrals on node 0
    results = comm.gather(local_integral, root=0)

    # Sum all local integrals for final integral
    if rank == 0:
        integral = sum(results)
        integral *= (b - a) / n
        exact = np.sin(b) - np.sin(a)
        return integral, exact


# MAIN
def main(args):
    """Main function"""
    args = argparser()
    trapezoid_scatter(args.lower, args.upper, args.steps)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
