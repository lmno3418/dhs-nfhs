# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# -------------------------------------------------------
# Column rename helper
# -------------------------------------------------------
def rename_columns(df, col_map):
    for old, new in col_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


# -------------------------------------------------------
# Silver: IR + MR harmonized
# -------------------------------------------------------
@dlt.table(
    name="imr_sil",
    comment="IR and MR harmonized individual-level Silver table",
    table_properties={"quality": "silver"}
)
def imr_sil():

    # Read Bronze via DLT
    ir = dlt.read("ir_brz")
    mr = dlt.read("mr_brz")

    # ---- Column maps (unchanged) ----
    col_map_ir = {
        "caseid": "person_id",
        "v001": "cluster_id",
        "v002": "household_id",
        "v003": "line_id",
        "v101": "state_id",
        "v005": "weight",
        "s731a": "milk_freq",
        "s731b": "pulses_freq",
        "s731c": "green_freq",
        "s731d": "fruit_freq",
        "s731e": "eggs_freq",
        "s731f": "fish_freq",
        "s731g": "meat_freq",
        "s731h": "fried_freq",
        "s731i": "soda_freq",
        "v481a": "ins_org",
        "v481b": "ins_emp",
        "v481c": "ins_ssec",
        "v481d": "ins_cp",
        "v481g": "ins_c",
        "v481x": "ins_o",
        "s728b": "hyp",
        "s728bb": "hyp_t"
    }

    col_map_mr = {
        "mcaseid": "person_id",
        "mv001": "cluster_id",
        "mv002": "household_id",
        "mv003": "line_id",
        "mv101": "state_id",
        "mv005": "weight",
        "sm630a": "milk_freq",
        "sm630b": "pulses_freq",
        "sm630c": "green_freq",
        "sm630d": "fruit_freq",
        "sm630e": "eggs_freq",
        "sm630f": "fish_freq",
        "sm630g": "meat_freq",
        "sm630h": "fried_freq",
        "sm630i": "soda_freq",
        "mv481a": "ins_org",
        "mv481b": "ins_emp",
        "mv481c": "ins_ssec",
        "mv481d": "ins_cp",
        "mv481g": "ins_c",
        "mv481x": "ins_o",
        "sm627b": "hyp",
        "sm627bb": "hyp_t"
    }

    ir_h = rename_columns(ir, col_map_ir).withColumn("sex", F.lit(2))
    mr_h = rename_columns(mr, col_map_mr).withColumn("sex", F.lit(1))

    common_cols = sorted(set(ir_h.columns).intersection(mr_h.columns))

    return ir_h.select(common_cols).unionByName(mr_h.select(common_cols))

# COMMAND ----------

@dlt.table(
    name="impr_sil",
    comment="Final individual merged PR + IR/MR Silver table",
    table_properties={"quality": "silver"}
)
def impr_sil():

    imr = dlt.read("imr_sil")
    pr  = dlt.read("pr_brz")

    # ---- PR column map (unchanged) ----
    col_map_pr = {
        "hv001": "cluster_id",
        "hv002": "household_id",
        "hvidx": "line_id",
        "hv024": "state_id",
        "hv005": "hh_weight",
        "hv025": "residence",
        "hv021": "psu_id",
        "hv270": "wealth_index",
        "hv271": "wealth_score",
        "sh49": "social_group",
        "hv105": "age",
        "hv106": "education",
        "sh25": "tobacco_use",
        "sh26": "alcohol_use",
        "shb18s": "sbp_1",
        "shb18d": "dbp_1",
        "shb25s": "sbp_2",
        "shb25d": "dbp_2",
        "shb29s": "sbp_3",
        "shb29d": "dbp_3",
        "shb21": "bp_medication",
        "shb53": "time_since_food",
        "shb54": "time_since_drink",
        "shb55": "glucose_tested",
        "shb56": "high_glucose_dx",
        "shb57": "glucose_medication",
        "shb73": "glucose_time",
        "shb74": "glucose_level",
        "ha2": "weight_kg",
        "ha3": "height_cm",
        "sh305": "waist_cm",
        "sh306": "hip_cm",
        "ha40": "bmi_women",
        "hb40": "bmi_men",
        "sh52": "cooking_fuel",
        "sh71": "hh_insurance",
        "sh73": "treatment_place"
    }

    pr_h = rename_columns(pr, col_map_pr)

    df = imr.join(
        pr_h,
        on=["cluster_id", "household_id", "line_id", "state_id"],
        how="left"
    )

    # Unified BMI
    df = df.withColumn("bmi", F.coalesce("bmi_women", "bmi_men")) \
           .drop("bmi_women", "bmi_men", "hhid", "hv104")

    # Casting logic 
    exclude = {
        "person_id", "waist_cm", "hip_cm", "weight", "hh_weight",
        "wealth_score", "bmi", "height_cm", "weight_kg", "glucose_time"
    }

    for c, t in df.dtypes:
        if c in exclude:
            if c != "person_id":
                df = df.withColumn(c, F.col(c).cast(DoubleType()))
        else:
            df = df.withColumn(c, F.col(c).cast(IntegerType()))

    return df