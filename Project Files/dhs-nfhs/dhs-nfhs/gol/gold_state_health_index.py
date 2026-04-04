# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_state_health_index",
    comment="Composite diabetes health index by state",
    table_properties={"quality":"gold"}
)

def gold_state_health_index():

    df = dlt.read("sil_nfhs_features")
    states = dlt.read("dim_states")

    state_stats = df.groupBy("state_id").agg(

        count("*").alias("population"),

        avg("combined_diabetes").alias("diabetes_prevalence"),

        avg("remission_flag").alias("remission_rate"),

        avg("bmi").alias("avg_bmi"),

        avg("diag_reported").alias("diagnosis_rate"),

        avg("measured_diabetes").alias("measured_rate")

    )

    state_stats = state_stats.withColumn(

        "diagnosis_gap",

        col("measured_rate") - col("diagnosis_rate")

    )

    state_stats = state_stats.withColumn(

        "health_index",

        (col("remission_rate") * 0.4) +
        ((1 - col("diabetes_prevalence")) * 0.3) +
        ((1 - col("diagnosis_gap")) * 0.2) +
        ((1 - (col("avg_bmi")/40)) * 0.1)

    )

    return state_stats.join(states,"state_id","left")