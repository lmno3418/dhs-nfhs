# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="sil_diabetes_indicators",
    table_properties={"quality":"silver"}
)

def sil_diabetes_indicators():

    df = dlt.read("sil_glucose_cleaned")

    df = df.withColumn(
    "measured_diabetes",
    when(
        (col("is_fasting")==1) &
        (col("glucose_level_clean")>=126),1
    )
    .when(
        (col("is_fasting")==0) &
        (col("glucose_level_clean")>=200),1
    )
    .when(
        col("glucose_level_clean")>=200,1
    )
    .otherwise(0)
    )


    # Prediabetes detection
    df = df.withColumn(
        "prediabetes",
        when(
            (col("measured_diabetes") == 0) &  # exclude diabetics
            (
                (
                    (col("is_fasting") == 1) &
                    (col("glucose_level_clean") >= 100) &
                    (col("glucose_level_clean") <= 125)
                ) |
                (
                    (col("is_fasting") == 0) &
                    (col("glucose_level_clean") >= 140) &
                    (col("glucose_level_clean") <= 199)
                )
            ),
            1
        ).otherwise(0)
    )


    df = df.withColumn(
        "diag_reported",
        when(col("high_glucose_dx")==1,1).otherwise(0)
    )

    df = df.withColumn(
        "on_med",
        when(col("glucose_medication")==1,1).otherwise(0)
    )

    df = df.withColumn(
        "combined_diabetes",
        when(
            (col("measured_diabetes")==1) |
            # (col("prediabetes") == 1) |
            (col("diag_reported")==1) |
            (col("on_med")==1),
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        "diabetes_category",
        when(col("combined_diabetes") == 1, "Diabetic")
        .when(col("prediabetes") == 1, "Prediabetic")
        .otherwise("Normal")
    )

    df = df.withColumn(
        "risk_level",
        when(col("combined_diabetes") == 1, "High Risk")
        .when(col("prediabetes") == 1, "Moderate Risk")
        .otherwise("Low Risk")
    )

    return df