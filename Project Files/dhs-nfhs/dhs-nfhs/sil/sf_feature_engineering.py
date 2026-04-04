# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="sil_nfhs_features",
    table_properties={"quality":"silver"}
)

def sil_nfhs_features():

    df = dlt.read("sil_diabetes_indicators")

    df = df.withColumn(
        "w_person",
        col("weight")/1000000
    )

    df = df.withColumn(
        "remission_flag",
        when(
            (col("diag_reported")==1) &
            (col("on_med")==0) &
            (col("measured_diabetes")==0),
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        "age_group",
        when(col("age")<25,"15-24")
        .when(col("age")<35,"25-34")
        .when(col("age")<45,"35-44")
        .when(col("age")<55,"45-54")
        .when(col("age")<65,"55-64")
        .otherwise("65+")
    )

    df = df.withColumn(
        "bmi_category",
        when(col("bmi")<18.5,"Underweight")
        .when(col("bmi")<25,"Normal")
        .when(col("bmi")<30,"Overweight")
        .otherwise("Obese")
    )

    return df

# COMMAND ----------

