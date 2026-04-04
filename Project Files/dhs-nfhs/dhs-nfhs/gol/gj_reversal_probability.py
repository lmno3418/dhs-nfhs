# Databricks notebook source
# import dlt
# from pyspark.sql.functions import *

# @dlt.table(
#     name="gold_reversal_probability",
#     comment="Diabetes reversal probability among diagnosed diabetics",
#     table_properties={"quality":"gold"}
# )

# def gold_reversal_probability():

#     df = dlt.read("sil_nfhs_features")

#     diagnosed = df.filter(col("diag_reported") == 1)

#     return diagnosed.groupBy(
#         "age_group",
#         "bmi_category",
#         "alcohol_use"
#     ).agg(

#         count("*").alias("diagnosed_population"),

#         sum("remission_flag").alias("remission_cases"),

#         avg("remission_flag").alias("reversal_probability")

#     )

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_reversal_probability",
    comment="Diabetes reversal probability among diagnosed diabetics (filtered & cleaned)",
    table_properties={"quality":"gold"}
)
def gold_reversal_probability():
    df = dlt.read("sil_nfhs_features")

    # normalize codes: treat 8/9 as null (example for alcohol/tobacco)
    alcohol_clean = when(col("alcohol_use").isin(8,9), None).otherwise(col("alcohol_use"))
    bmi_cat = col("bmi_category")
    age_grp = col("age_group")

    diagnosed = df.filter(col("diag_reported").cast("int") == 1) \
                  .select(age_grp.alias("age_group"), bmi_cat.alias("bmi_category"),
                          alcohol_clean.alias("alcohol_use"), col("remission_flag").cast("int"))

    agg = diagnosed.groupBy("age_group", "bmi_category", "alcohol_use").agg(
        count("*").alias("diagnosed_population"),
        sum("remission_flag").alias("remission_cases"),
        avg("remission_flag").alias("reversal_probability")
    )

    # keep only groups with enough samples to be stable (tune threshold)
    min_n = 30
    return agg.where(col("diagnosed_population") >= min_n)