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
import numpy as np

# FUNCTIONS
def sieve_erat(n):
    """ Desc """
    end = int(np.sqrt(n))
    bools = np.ones(n-2)
    for i in range(0, end):
        if bools[i]:
            num = i+2
            for j in range(num**2, n, num):
                bools[j-2] = 0
    # end_sum=0
    # for i,booly in enumerate(bools):
    #     if booly == 1:
    #         end_sum += i+2
    # print(end_sum)
    return bools


# MAIN
def main(args):
    """ Main function """
    # FINISH
    sieve_erat(4000)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
