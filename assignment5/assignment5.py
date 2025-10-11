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
import pyspark.sql.functions as sf
from Bio import SeqIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

def argparser():
    """Argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", default=2, type=str)
    return parser.parse_args()


def process_batch(insertion_list, table_name, spark, schema):
    """
    Create table if missing and insert a batch of records into a Spark SQL/Hive table.
    """
    rdd = spark.sparkContext.parallelize(insertion_list, numSlices=1)
    if not insertion_list:
        return
    df = rdd.toDF(schema=schema)

    # Check if the table exists
    existing_tables = [t.name for t in spark.catalog.listTables()]
    if table_name not in existing_tables:
        # Create empty table with same schema
        df.limit(0).write.saveAsTable(table_name, schema=schema)

    # Append data to the table
    df.write.mode("append").saveAsTable(table_name)


def parse_genbank(ref_path, batch_size=10000):
    """Go through genbank format file and store species and proteins into database"""

    # Schema for feature table
    feature_schema = StructType(
        [
            StructField("loc", StringType(), True),
            StructField("len", IntegerType(), True),
            StructField("ftype", StringType(), True),
        ]
    )

    # Schema for species table
    species_schema = StructType(
        [
            StructField("acc_id", StringType(), True),
            StructField("genome_size", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("num_genes", IntegerType(), True),
            StructField("num_proteins", IntegerType(), True),
            StructField("num_features", IntegerType(), True),
            StructField("taxdb", StringType(), True),
        ]
    )

    # spark = (
    #     SparkSession.builder.appName("assignment5_ogkourlias")
    #     .master("spark://spark.bin.bioinf.nl:7077")
    #     .config("spark.executor.memory", "16g")
    #     .getOrCreate()
    # )

    spark = (
        SparkSession.builder.appName("assignment5_ogkourlias")
        .master("local[*]")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    sc = spark.sparkContext

    species_buffer = []
    feature_buffer = []

    for record in SeqIO.parse(ref_path, format="genbank"):
        record_id = str(record.id)
        genome_size = int(len(record))
        annotations = record.annotations
        species_name = str(annotations.get("organism", [None]))
        features = record.features

        # Record/species dict init
        record_dict = {
            "acc_id": record_id,
            "genome_size": genome_size,
            "name": species_name,
            "num_genes": 0,
            "num_proteins": 0,
            "num_features": 0,
        }

        # Find gene specifics in features
        for feature in features:
            record_dict["num_features"] += 1
            ftype = feature.type

            if ftype == "gene":
                record_dict["num_genes"] += 1
            elif ftype == "CDS":
                record_dict["num_proteins"] += 1

            feature_dict = {
                "loc": str(feature.location),
                "len": int(len(feature)),
                "ftype": str(ftype),
            }

            feature_buffer.append(feature_dict)

            # Insert into table and flush buffer list
            if len(feature_buffer) >= batch_size:
                process_batch(feature_buffer, "features", spark, feature_schema)
                feature_buffer.clear()

        species_buffer.append(record_dict)

        # Insert into table and flush buffer list
        if len(species_buffer) >= batch_size:
            process_batch(species_buffer, "species", spark, species_schema)
            species_buffer.clear()

    # Insert any leftovers into tables
    if species_buffer:
        process_batch(species_buffer, "species", spark, species_schema)

    if feature_buffer:
        process_batch(feature_buffer, "features", spark, feature_schema)

    return spark


# MAIN
def main(args):
    """Main function"""
    args = argparser()
    spark = parse_genbank(args.input, batch_size=5000)
    species_df = spark.read.table("species")
    features_df = spark.read.table("features")
    print(
        f"Archaeal genome average number of features: {species_df.select(sf.mean('num_features')).collect()[0][0]}"
    )
    print(
        f"Max number of proteins out of all genomes: {species_df.select(sf.max('num_proteins')).collect()[0][0]}"
    )
    print(
        f"Min number of proteins out of all genomes: {species_df.select(sf.min('num_proteins')).collect()[0][0]}"
    )
    print(
        f"Proteins/Features ratio: {species_df.select(sf.try_divide('num_proteins', 'num_features')).collect()[0][0]}"
    )
    print(
        f"Archaeal genome average feature length: {features_df.select(sf.mean('len')).collect()[0][0]}"
    )
    filtered_df = features_df.filter(
        (~features_df["loc"].contains("<"))
        & (~features_df["loc"].contains(">"))
        & (features_df["ftype"] == "CDS")
    )
    print(
        f"Filtered average feature length: {filtered_df.select(sf.mean('len')).collect()[0][0]}"
    )
    # FINISH
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
