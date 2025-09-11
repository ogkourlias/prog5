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
    h = (b-a) / n
    integral = 0.0
    for index in range(n+1):
        x_i = a + index * h
        if index == 0 or index == n:
            integral += np.cos(x_i)
        else:
            integral += 2.0 * np.cos(x_i)
    integral *= h/2
    exact = np.sin(b) - np.sin(a)
    diff = abs(integral - exact)
    return n, diff


# MAIN
def main(args):
    """ Main function """
    args = argparser()
    print(trapezoid(args.lower, args.upper, args.steps))
    # FINISH
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
