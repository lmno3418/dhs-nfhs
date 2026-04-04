# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_glucose_distribution",
    table_properties={"quality":"gold"}
)

def gold_glucose_distribution():

    df = dlt.read("sil_nfhs_features")

    return df.select(
        "bmi",
        "glucose_level_clean"
    ).where(col("glucose_level_clean").isNotNull())