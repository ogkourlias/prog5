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
import os
import sys
import argparse
import configparser

sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from functools import reduce
import operator


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", required=True, type=str,
                        help="input TSV file path")
    return parser.parse_args()

def create_spark(app_name: str = "assignment6_ogkourlias") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", "/students/2025-2026/master/orfeas/prog5/assignment6/mariadb-java-client-3.5.6.jar")
        .config("spark.executor.memory", "16g")
        .config("spark.executor.cores", "4")
        .config("spark.local.dir", "/students/2025-2026/master/orfeas/prog5/assignment6/spark-temp")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

def read_db_credentials():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.my.cnf"))
    user = cfg.get("client", "user")
    password = cfg.get("client", "password")
    return user, password

def parse_file(spark: SparkSession, input_f: str, user: str, password: str):
    df = spark.read.csv(input_f, sep="\t", header=True, inferSchema=False)

    pred_cols = [c for c in df.columns if "pred" in c.lower()]

    df = df.repartition(8)

    missing_count_exprs = [
        F.sum(when(col(c).isin(".", ".;"), 1).otherwise(0)).alias(c)
        for c in pred_cols
    ]
    counts_row = df.select(*missing_count_exprs).collect()[0].asDict()

    sorted_preds = sorted(pred_cols, key=lambda c: counts_row.get(c, 0))
    top5 = sorted_preds[:5]

    id_keys = ["#chr", "pos(1-based)", "Ensembl_proteinid"]
    present_id_keys = [k for k in id_keys if k in df.columns]

    df = df.dropDuplicates(present_id_keys)

    df = df.withColumn("snp_chr", concat_ws("_", col("#chr"), col("pos(1-based)")))

    nan_flags = [when(col(c).isin(".", ".;"), 1).otherwise(0) for c in top5]
    nan_count_expr = reduce(operator.add, nan_flags)
    df = df.withColumn("nan_count", nan_count_expr)

    pred_select_cols = [col(c) for c in top5]
    pred_df = df.select(*pred_select_cols, col("nan_count"), col("snp_chr"), col("Ensembl_proteinid"))

    snp_df = df.select("snp_chr").distinct()
    protein_df = df.select("Ensembl_proteinid").distinct()

    snp_window = Window.orderBy("snp_chr")
    snp_df = snp_df.withColumn("snp_id", row_number().over(snp_window))
    snp_df = snp_df.withColumn("snp_id", monotonically_increasing_id())

    protein_window = Window.orderBy("Ensembl_proteinid")
    protein_df = protein_df.withColumn("protein_id", row_number().over(protein_window))
    protein_df = protein_df.withColumn("protein_id", monotonically_increasing_id())

    pred_window = Window.orderBy("snp_chr", "Ensembl_proteinid")
    pred_df = pred_df.withColumn("id", row_number().over(pred_window))

    pred_df = pred_df.join(snp_df, on="snp_chr", how="left")
    pred_df = pred_df.join(protein_df, on="Ensembl_proteinid", how="left")
    pred_df = pred_df.withColumn("id", monotonically_increasing_id())

    pred_final_cols = ["id"] + top5 + ["snp_id", "protein_id"]
    pred_df = pred_df.select(*pred_final_cols)
    
    jdbc_url = "jdbc:mariadb://mariadb.bin.bioinf.nl/Ogkourlias"
    jdbc_props = {
        "driver": "org.mariadb.jdbc.Driver",
        "user": user,
        "password": password,
    }

    pred_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", jdbc_url) \
        .option("dbtable", "pred_table") \
        .option("driver", jdbc_props["driver"]) \
        .option("user", jdbc_props["user"]) \
        .option("password", jdbc_props["password"]) \
        .option("batchsize", "10000") \
        .option("numPartitions", "8") \
        .save()

    snp_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", jdbc_url) \
        .option("dbtable", "snp_table") \
        .option("driver", jdbc_props["driver"]) \
        .option("user", jdbc_props["user"]) \
        .option("password", jdbc_props["password"]) \
        .option("batchsize", "10000") \
        .option("numPartitions", "4") \
        .save()

    protein_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", jdbc_url) \
        .option("dbtable", "protein_table") \
        .option("driver", jdbc_props["driver"]) \
        .option("user", jdbc_props["user"]) \
        .option("password", jdbc_props["password"]) \
        .option("batchsize", "10000") \
        .option("numPartitions", "4") \
        .save()


def main():
    args = argparser()
    user, password = read_db_credentials()
    spark = create_spark()
    parse_file(spark, args.input, user, password)


if __name__ == "__main__":
    main()
