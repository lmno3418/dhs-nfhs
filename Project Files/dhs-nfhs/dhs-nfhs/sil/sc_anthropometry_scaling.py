# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="sil_anthropometry",
    table_properties={"quality":"silver"}
)

def sil_anthropometry():

    df = dlt.read("sil_cleaned_base")

    df = df.withColumn("bmi", col("bmi")/100)

    df = df.withColumn("height_cm", col("height_cm")/10)

    df = df.withColumn("weight_kg", col("weight_kg")/10)

    df = df.withColumn(
        "bmi",
        when(col("bmi")>60,None).otherwise(col("bmi"))
    )

    return df