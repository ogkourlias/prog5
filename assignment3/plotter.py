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
from matplotlib import pyplot as plt
import pandas as pd

# FUNCTIONS
def plotter(path):
    df = pd.read_csv(path, index_col=None, skipinitialspace=True)
    df["systime"] = df["systime"].str.split(":").str[1].astype(float)
    print(df["systime"])
    plt.plot(df["nodes"], df["systime"], "-o")
    plt.title("Runtime of mpi4py implementation of Sieve of Eratosthenes\n"
            r"$N = 5 \times 10^{8}$")
    plt.xlabel("Number of nodes (nodes)")
    plt.ylabel("Execution time (seconds)")
    plt.savefig("nodes_plot.png")

# MAIN
def main(args):
    """ Main function """
    # FINISH
    plotter("node_results.csv")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
