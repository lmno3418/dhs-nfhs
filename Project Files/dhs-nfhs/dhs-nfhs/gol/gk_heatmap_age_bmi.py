# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_heatmap_age_bmi",
    table_properties={"quality":"gold"}
)

def gold_heatmap_age_bmi():

    df = dlt.read("sil_nfhs_features")

    return df.groupBy(
        "age_group",
        "bmi_category"
    ).agg(

        avg("remission_flag").alias("remission_probability")

    )