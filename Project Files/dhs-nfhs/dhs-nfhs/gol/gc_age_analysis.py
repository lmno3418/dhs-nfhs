# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_age_analysis",
    table_properties={"quality":"gold"}
)

def gold_age_analysis():

    df = dlt.read("sil_nfhs_features")

    return df.groupBy("age_group").agg(

        count("*").alias("population"),

        sum("combined_diabetes").alias("diabetes_cases"),

        sum("remission_flag").alias("remission_cases"),

        avg("combined_diabetes").alias("diabetes_prevalence"),

        avg("remission_flag").alias("remission_rate")

    )