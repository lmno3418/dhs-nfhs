# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_dashboard_kpi",
    comment="Main KPI metrics for diabetes dashboard",
    table_properties={"quality":"gold"}
)

def gold_dashboard_kpi():

    df = dlt.read("sil_nfhs_features")

    return df.select(
        count("*").alias("total_population"),

        sum(when(col("glucose_level_clean").isNotNull(),1)).alias("tested_population"),

        sum("measured_diabetes").alias("measured_diabetes_cases"),

        sum("diag_reported").alias("diagnosed_cases"),

        sum("on_med").alias("medication_cases"),

        sum("combined_diabetes").alias("total_diabetes_cases"),

        sum("remission_flag").alias("remission_cases"),

        avg("combined_diabetes").alias("diabetes_prevalence"),

        avg("remission_flag").alias("remission_rate")
    )