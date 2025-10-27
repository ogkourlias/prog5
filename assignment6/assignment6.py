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

import argparse
import sys
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

import pyspark.sql.functions as sf
from Bio import SeqIO
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import isnan, when, count, col, concat_ws, sum, coalesce, lit, length, regexp_replace, monotonically_increasing_id
from functools import reduce

def argparser():
    """Argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", default=2, type=str)
    return parser.parse_args()

def parse_file(input_f):
    # Create SparkSession
    spark = (
        SparkSession.builder.appName("assignment6_ogkourlias")
        # .master("spark://spark.bin.bioinf.nl:7077")
        .master("local[*]")
        .config("spark.jars", "/path/to/mariadb-java-client-3.5.0.jar")
        .config("spark.executor.memory", "8g")
        .getOrCreate()
    )

    # Read CSV File
    df = spark.read.csv(input_f, sep="\t", header=True)
    pred_cols = np.array([col for col in df.columns if "pred" in col.lower()])
    df = df.repartition(*pred_cols)
    counts = [df.filter((col(column) == ".") | (col(column) == ".;")).count() for column in pred_cols]
    desc_idx = np.argsort(counts)
    top_pred_cols = pred_cols[desc_idx[:5]]
    top_pred_cols = np.concatenate((["#chr", "pos(1-based)", "Ensembl_proteinid"], top_pred_cols))
    df = df.select(*top_pred_cols)

    col_list = [col(column) for column in top_pred_cols[3:]]
    df = df.withColumn("snp_chr", concat_ws("_", col("#chr"), col("pos(1-based)")))
    df = df.withColumn("pred_str", concat_ws("", *col_list))
    df = df.withColumn("nan_count", length("pred_str") - length(regexp_replace("pred_str", r'\.;|\.', "")))
    df = df.sort(col("nan_count"), ascending=True)
    df = df.dropDuplicates()
    pred_df = df.select([*col_list, col("nan_count"), col("snp_chr"), col("Ensembl_proteinid")])
    snp_df = df.select([col("snp_chr")]).dropDuplicates()
    protein_df = df.select([col("Ensembl_proteinid")]).dropDuplicates()
    pred_df = pred_df.withColumn("id", monotonically_increasing_id())
    snp_df = snp_df.withColumn("snp_id", monotonically_increasing_id())
    protein_df = protein_df.withColumn("protein_id", monotonically_increasing_id())
    pred_df = pred_df.join(snp_df, on="snp_chr", how="left")
    pred_df = pred_df.join(protein_df, on="Ensembl_proteinid", how="left")
    pred_df = pred_df.select(["id", *col_list, "snp_id","protein_id"])

    # writing to mariadb
    pred_df.createOrReplaceTempView("pred_view")
    pred_df.write.mode("overwrite").saveAsTable("Ogkourlias.pred_table").option("driver","org.mariadb.jdbc.Driver").option("url", f"jdbc:mariadb://mariadb.bin.bioinf.nl/Ogkourlias")


    
def main(args):
    """Main function"""
    args = argparser()
    parse_file(args.input)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
