# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_residence_analysis",
    table_properties={"quality":"gold"}
)

def gold_residence_analysis():

    df = dlt.read("sil_nfhs_features")

    return df.groupBy("residence").agg(

        count("*").alias("population"),

        sum("combined_diabetes").alias("diabetes_cases"),

        avg("combined_diabetes").alias("diabetes_prevalence")

    )