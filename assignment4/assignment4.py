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
from Bio import SeqIO
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, Integer, String, VARCHAR
if sqlalchemy.__version__.startswith('1.4'):
    from sqlalchemy import create_engine
    from sqlalchemy.engine import make_url, URL
    from sqlalchemy.sql import text
elif sqlalchemy.__version__.startswith('2'):
    from sqlalchemy import create_engine
    from sqlalchemy.url import make_url, URL
    from sqlalchemy.sql import text
    



def argparser():
    """Argument parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", default=2, type=str)
    return parser.parse_args()

def make_engine():
    myDB = URL.create(
    drivername="mariadb",
    query={"read_default_file": os.path.expanduser("~/.my.cnf")}
    )
    engine = create_engine(myDB, echo=False)
    return engine

def create_species_table(engine): # Species name, Species acc, Genome size, Number of genes, Number of proteins, TaxDB
    metadata = MetaData()
    species = Table(
    "species", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", VARCHAR(255), nullable=True),
    Column("acc_id", VARCHAR(255), nullable=True),
    Column("genome_size", Integer, nullable=True),
    Column("num_genes", Integer, nullable=True),
    Column("num_proteins", Integer, nullable=True),
    Column("taxdb", VARCHAR(255), nullable=True)
)
    species.drop(engine, checkfirst=True)
    metadata.create_all(engine)
    return species


def create_protein_table(engine):  # Protein ID, Protein Product, Protein Location, Locus Tag, Gene ref, EC number, GO Annotation
    metadata = MetaData()
    proteins = Table(
    "proteins", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", VARCHAR(255), nullable=True),
    Column("protein_id", VARCHAR(255), nullable=True),
    Column("loc", VARCHAR(255), nullable=True),
    Column("loc_tag", VARCHAR(255), nullable=True),
    Column("gene_ref", VARCHAR(255), nullable=True),
    Column("ec_num", VARCHAR(255), nullable=True),
    Column("go_annot", VARCHAR(255), nullable=True),
    Column("go_func", VARCHAR(255), nullable=True)
)
    proteins.drop(engine, checkfirst=True)
    metadata.create_all(engine)
    return proteins

def create_link_table(engine):  # Protein ID, Protein Product, Protein Location, Locus Tag, Gene ref, EC number, GO Annotation
    metadata = MetaData()
    species_protein = Table(
    "species_protein", metadata,
    Column("id", Integer, primary_key=True),
    Column("protein_id", VARCHAR(255), nullable=True),
    Column("species_id", VARCHAR(255), nullable=True)
)   
    species_protein.drop(engine, checkfirst=True)
    metadata.create_all(engine)
    return species_protein

def process_batch(connection, table, insertion_list):
    connection.execute(table.insert(), insertion_list)

def parse_genbank(ref_path, engine, species_table, protein_table, link_table, batch_size = 5000):
    species_buffer = []  # Species name, Species acc, Genome size, Number of genes, Number of proteins, TaxDB
    protein_buffer = []  # Protein ID, Protein Product, Protein Location, Locus Tag, Gene ref, EC number, GO Annotation
    link_buffer = []

    for record in SeqIO.parse(ref_path, format="genbank"):
        record_id = record.id
        genome_size = len(record)
        annotations = record.annotations
        species_name = annotations.get("organism", "")
        features = record.features

        # Record/species dict init
        record_dict = {
            "acc_id": record_id,
            "genome_size": genome_size,
            "name": species_name,
            "num_genes": 0,
            "num_proteins": 0
        }

        # Find gene specifics in features
        for feature in features:
            ftype = feature.type
            qualifiers = feature.qualifiers
            
            if ftype == "source":
                record_dict["taxdb"] = qualifiers.get("db_xref")# TaxDB Id
            
            elif ftype == "gene":
                record_dict["num_genes"] += 1

            elif ftype == "CDS":
                record_dict["num_proteins"] += 1
                protein_id = qualifiers.get("protein_id", [None])[0]
                link_dict = {"protein_id" : protein_id,
                            "species_id" : record_id}
                protein_dict = {
                    "protein_id": protein_id,
                    "name": qualifiers.get("product", [""])[0],
                    "loc": str(feature.location),
                    "loc_tag": qualifiers.get("locus_tag", [None])[0],
                    "gene_ref": qualifiers.get("inference", [""])[0].split(":")[-1],
                    "go_annot": qualifiers.get("GO_process", [None])[0],
                    "go_func": qualifiers.get("GO_function", [None])[0],
                    "ec_num": qualifiers.get("EC_number", [None])[0]
                }
                protein_buffer.append(protein_dict)
                link_buffer.append(link_dict)

                # Insert into table and flush buffer list
                if len(protein_buffer) >= batch_size:
                    process_batch(engine, protein_table, protein_buffer)
                    protein_buffer.clear()
                    
                # Insert into table and flush buffer list
                if len(link_buffer) >= batch_size:
                    process_batch(engine, link_table, link_buffer)
                    link_buffer.clear()

        species_buffer.append(record_dict)

        # Insert into table and flush buffer list
        if len(species_buffer) >= batch_size:
            process_batch(engine, species_table, species_buffer)
            species_buffer.clear()

    # Insert any leftovers into tables
    if species_buffer:
        process_batch(engine, species_table, species_buffer)

    if protein_buffer:
        process_batch(engine, protein_table, protein_buffer)

    if link_buffer:
        process_batch(engine, link_table, link_buffer)

# MAIN
def main(args):
    """Main function"""
    args = argparser()
    engine = make_engine()
    species_table = create_species_table(engine)
    protein_table = create_protein_table(engine)
    link_table = create_link_table(engine)
    parse_genbank(args.input, engine, species_table, protein_table, link_table, batch_size=5000)
    # FINISH
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
