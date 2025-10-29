#!/bin/bash
#SBATCH -c 4
#SBATCH --ntasks=4
#SBATCH --mem=16gb
#SBATCH --time=5:59:59

mysql <<EOF
SET FOREIGN_KEY_CHECKS=0;

TRUNCATE TABLE pred_table;
TRUNCATE TABLE snp_table;
TRUNCATE TABLE protein_table;

SET FOREIGN_KEY_CHECKS=1;

CREATE TABLE IF NOT EXISTS snp_table (
    snp_id BIGINT PRIMARY KEY,
    snp_chr TEXT
);

CREATE TABLE IF NOT EXISTS protein_table (
    protein_id BIGINT PRIMARY KEY,
    Ensembl_proteinid TEXT
);

CREATE TABLE IF NOT EXISTS pred_table (
    id BIGINT PRIMARY KEY,
    snp_id BIGINT,
    protein_id BIGINT,
    nan_count INT,
    pred1 TEXT,
    pred2 TEXT,
    pred3 TEXT,
    pred4 TEXT,
    pred5 TEXT,
    FOREIGN KEY (snp_id) REFERENCES snp_table(snp_id),
    FOREIGN KEY (protein_id) REFERENCES protein_table(protein_id)
);
EOF

./assignment6.py -i dbsnp.txt.gz
