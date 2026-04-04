# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="sil_cleaned_base",
    table_properties={"quality":"silver"}
)

def sil_cleaned_base():

    df = dlt.read("impr_sil")

    df = df.withColumn("age", col("age").cast("int"))

    df = df.withColumn("weight", col("weight").cast("double"))

    df = df.withColumn("time_since_food", col("time_since_food").cast("double"))

    return df