# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_lifestyle_analysis",
    table_properties={"quality":"gold"}
)

def gold_lifestyle_analysis():

    df = dlt.read("sil_nfhs_features")

    return df.groupBy(
        "fruit_freq",
        "alcohol_use",
        "tobacco_use"
    ).agg(

        count("*").alias("population"),

        avg("combined_diabetes").alias("diabetes_prevalence"),

        avg("remission_flag").alias("remission_rate")

    )