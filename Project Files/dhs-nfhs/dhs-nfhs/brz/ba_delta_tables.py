# Databricks notebook source
# put imports at the top
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# then your bronze table declarations
@dlt.table(
    name="ir_brz",
    comment="NFHS IR Bronze table (from cleaned CSV)",
    table_properties={"quality": "bronze"}
)
def ir_brz():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/dhs-nfhs/data/clean_data/IR_clean.csv")
    )

@dlt.table(
    name="mr_brz",
    comment="NFHS MR Bronze table (from cleaned CSV)",
    table_properties={"quality": "bronze"}
)
def mr_brz():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/dhs-nfhs/data/clean_data/MR_clean.csv")
    )

@dlt.table(
    name="pr_brz",
    comment="NFHS PR Bronze table (from cleaned CSV)",
    table_properties={"quality": "bronze"}
)
def pr_brz():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/dhs-nfhs/data/clean_data/PR_clean.csv")
    )