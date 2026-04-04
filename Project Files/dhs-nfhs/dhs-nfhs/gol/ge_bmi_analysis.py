# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_bmi_analysis",
    table_properties={"quality":"gold"}
)

def gold_bmi_analysis():

    df = dlt.read("sil_nfhs_features")

    return df.groupBy("bmi_category").agg(

        count("*").alias("population"),

        sum("remission_flag").alias("remission_cases"),

        avg("remission_flag").alias("remission_rate")

    )