# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_care_cascade",
    table_properties={"quality":"gold"}
)

def gold_care_cascade():

    df = dlt.read("sil_nfhs_features")

    return df.select(

        count("*").alias("total_population"),

        sum(when(col("glucose_level_clean").isNotNull(),1)).alias("tested"),

        sum("measured_diabetes").alias("measured_diabetes"),

        sum("diag_reported").alias("diagnosed"),

        sum("on_med").alias("on_medication"),

        sum("remission_flag").alias("remission")

    )