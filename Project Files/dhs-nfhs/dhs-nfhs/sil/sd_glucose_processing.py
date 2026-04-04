# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="sil_glucose_cleaned",
    table_properties={"quality":"silver"}
)

def sil_glucose_cleaned():

    df = dlt.read("sil_anthropometry")

    sentinels = [995,996,998,999,9996,9998,9999,99999]

    df = df.withColumn(
    "glucose_level_clean",
    when(
        (col("glucose_level").isin(sentinels)) |
        (col("glucose_level") <= 0),
        None
    ).otherwise(col("glucose_level"))
    )

    df = df.withColumn(
        "time_since_food_hr",
        col("time_since_food").cast("double")
    )

    df = df.withColumn(
        "is_fasting",
        when(col("time_since_food_hr")>=8,1)
        .when(col("time_since_food_hr").isNotNull(),0)
    )

    return df