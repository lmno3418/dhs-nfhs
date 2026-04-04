# Databricks notebook source
import os
import zipfile
from pyspark.sql.types import StringType
from pyspark.sql.functions import input_file_name

# Clean data files to check
clean_files = [
    "/Volumes/dhs-nfhs/data/clean_data/MR_clean.csv",
    "/Volumes/dhs-nfhs/data/clean_data/IR_clean.csv",
    "/Volumes/dhs-nfhs/data/clean_data/PR_clean.csv"
]

# If all clean files exist, skip execution
if all(os.path.exists(f) for f in clean_files):
    print("Clean CSV files already exist. Skipping unzip step.")
else:
    base_dir = 'dhs-nfhs.data.zips'
    output_base = '/Volumes/dhs-nfhs/data/raw_data'

    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                folder_name = os.path.splitext(file)[0]
                output_folder = os.path.join(output_base, folder_name)

                os.makedirs(output_folder, exist_ok=True)

                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(output_folder)

                print(f"Extracted: {zip_path} -> {output_folder}")