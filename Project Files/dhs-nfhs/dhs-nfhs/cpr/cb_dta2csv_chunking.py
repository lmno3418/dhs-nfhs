# Databricks notebook source
# --------------------------------------------------------------------------------
# 1. HOUSEHOLD MEMBER RECODE (PR) - The Clinical/Biomarker Master
# --------------------------------------------------------------------------------
pr_cols_requested = [
    # --- Identification & Weights ---
    "hhid",
    "hv001", "hv002", "hvidx", "hv024",  #hv003
    "hv005", "hv021", "hv025", "hv270", "hv271", "sh49", "sh52", "sh71", "sh73", "hv104", "hv105", "hv106", 
    "sh25", "sh26", "shb18s", "shb18d", "shb21", "shb25s", "shb25d", "shb29s", "shb29d",
    "shb53","shb54","shb55","shb56","shb57","shb73","shb74","sh305","sh306","ha2","ha3",
    "ha40","hb40"
]

# --------------------------------------------------------------------------------
# 2. WOMEN'S RECODE (IR) - Detailed Lifestyle & History
# --------------------------------------------------------------------------------
ir_cols_requested = [
    "caseid", 
    "v001", "v002", "v003", "v101",
    "v005", "s731a", "s731b", "s731c", "s731d", "s731e", "s731f", "s731g", "s731h", "s731i",
    "v481a", "v481b", "v481c", "v481d", "v481g", "v481x", "s728b", "s728bb"
]

# --------------------------------------------------------------------------------
# 3. MEN'S RECODE (MR) - Detailed Lifestyle & History
# --------------------------------------------------------------------------------
mr_cols_requested = [
    "mcaseid", 
    "mv001", "mv002", "mv003", "mv101",
    "mv005", "sm630a", "sm630b", "sm630c", "sm630d", "sm630e", "sm630f", "sm630g", "sm630h", "sm630i",
    "mv481a", "mv481b", "mv481c", "mv481d", "mv481g", "mv481x", "sm627b", "sm627bb"
]

# COMMAND ----------

import pandas as pd
import numpy as np
import os

def clean_dhs_chunk(df, keep_cols):
    """
    Clean a chunk of DHS data:
    - keep only requested columns
    - cast everything to string
    """
    df = df[keep_cols].copy()

    for col in df.columns:
        df[col] = df[col].astype("string")

    return df


def dta_to_clean_csv_chunked_databricks(
    dta_path,
    output_dir,
    keep_cols,
    chunksize=10000
):
    """
    Databricks-safe DTA → CSV conversion.
    Writes ONE CSV PER CHUNK (no append).
    """
    print(f"Processing: {os.path.basename(dta_path)}")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    chunk_iter = pd.read_stata(
        dta_path,
        convert_categoricals=False,
        chunksize=chunksize
    )

    for i, chunk in enumerate(chunk_iter, start=1):
        print(f"  → Chunk {i}")

        clean_chunk = clean_dhs_chunk(chunk, keep_cols)

        chunk_path = os.path.join(
            output_dir,
            f"part_{i:05d}.csv"
        )

        clean_chunk.to_csv(
            chunk_path,
            index=False,
            encoding="utf-8"
        )

    print(f"Finished writing chunks to: {output_dir}\n")


# COMMAND ----------

import os

pr_output_file = "/Volumes/dhs-nfhs/data/clean_data/PR_clean.csv"
ir_output_file = "/Volumes/dhs-nfhs/data/clean_data/IR_clean.csv"
mr_output_file = "/Volumes/dhs-nfhs/data/clean_data/MR_clean.csv"


# PR
if os.path.exists(pr_output_file):
    print("PR_clean.csv already exists. Skipping PR processing.")
else:
    dta_to_clean_csv_chunked_databricks(
        dta_path="/Volumes/dhs-nfhs/data/raw_data/IAPR7EDT/IAPR7EFL.DTA",
        output_dir="/Volumes/dhs-nfhs/data/clean_data/PR_clean",
        keep_cols=pr_cols_requested,
        chunksize=10000
    )


# IR
if os.path.exists(ir_output_file):
    print("IR_clean.csv already exists. Skipping IR processing.")
else:
    dta_to_clean_csv_chunked_databricks(
        dta_path="/Volumes/dhs-nfhs/data/raw_data/IAIR7EDT/IAIR7EFL.DTA",
        output_dir="/Volumes/dhs-nfhs/data/clean_data/IR_clean",
        keep_cols=ir_cols_requested,
        chunksize=10000
    )


# MR
if os.path.exists(mr_output_file):
    print("MR_clean.csv already exists. Skipping MR processing.")
else:
    dta_to_clean_csv_chunked_databricks(
        dta_path="/Volumes/dhs-nfhs/data/raw_data/IAMR7EDT/IAMR7EFL.DTA",
        output_dir="/Volumes/dhs-nfhs/data/clean_data/MR_clean",
        keep_cols=mr_cols_requested,
        chunksize=10000
    )