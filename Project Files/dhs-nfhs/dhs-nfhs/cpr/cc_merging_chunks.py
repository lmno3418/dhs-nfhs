# Databricks notebook source
import os

def merge_chunk_csvs(
    parts_dir,
    output_csv_name,
    parts_prefix="part_",
    encoding="utf-8"
):
    """
    Merge chunk CSVs (part_00001.csv, part_00002.csv, ...)
    into a single CSV in the same directory.
    Skips execution if output file already exists.
    """

    output_path = output_csv_name  # since you're passing full path

    # Skip if output already exists
    if os.path.exists(output_path):
        print(f"Skipping: {output_path} already exists\n")
        return

    # Collect and sort chunk files
    part_files = sorted(
        f for f in os.listdir(parts_dir)
        if f.startswith(parts_prefix) and f.endswith(".csv")
    )

    if not part_files:
        raise ValueError(f"No chunk CSVs found in {parts_dir}")

    print(f"Merging {len(part_files)} files → {output_path}")

    with open(output_path, "wb") as outfile:
        for i, fname in enumerate(part_files):
            part_path = os.path.join(parts_dir, fname)
            with open(part_path, "rb") as infile:
                if i > 0:
                    infile.readline()  # skip header
                outfile.write(infile.read())

    print("Merge complete\n")


merge_chunk_csvs(
    parts_dir="/Volumes/dhs-nfhs/data/clean_data/PR_clean",
    output_csv_name="/Volumes/dhs-nfhs/data/clean_data/PR_clean.csv"
)

merge_chunk_csvs(
    parts_dir="/Volumes/dhs-nfhs/data/clean_data/IR_clean",
    output_csv_name="/Volumes/dhs-nfhs/data/clean_data/IR_clean.csv"
)

merge_chunk_csvs(
    parts_dir="/Volumes/dhs-nfhs/data/clean_data/MR_clean",
    output_csv_name="/Volumes/dhs-nfhs/data/clean_data/MR_clean.csv"
)