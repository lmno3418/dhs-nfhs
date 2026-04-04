# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_state_summary",
    table_properties={"quality":"gold"}
)

def gold_state_summary():

    df = dlt.read("sil_nfhs_features")
    states = dlt.read("dim_states")

    summary = df.groupBy("state_id").agg(

        count("*").alias("population"),

        sum("combined_diabetes").alias("diabetes_cases"),

        sum("remission_flag").alias("remission_cases"),

        avg("combined_diabetes").alias("diabetes_prevalence"),

        avg("remission_flag").alias("remission_rate")

    )

    return summary.join(states, "state_id", "left")