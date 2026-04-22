import dlt
from pyspark.sql.functions import *

# =========================
# CUSTOMERS
# =========================
@dlt.table(
    name="bronze_customers",
    comment="Raw customers data"
)
def bronze_customers():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://telecom-bucket-135/customers (1).csv")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


# =========================
# COMPLAINTS
# =========================
@dlt.table(name="bronze_complaints")
def bronze_complaints():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://telecom-bucket-135/complaints.csv")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


# =========================
# CAMPAIGNS
# =========================
@dlt.table(name="bronze_campaigns")
def bronze_campaigns():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://telecom-bucket-135/campaigns.csv")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


# =========================
# PLANS
# =========================
@dlt.table(name="bronze_plans")
def bronze_plans():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://telecom-bucket-135/plans.csv")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


# =========================
# RECHARGES
# =========================
@dlt.table(name="bronze_recharges")
def bronze_recharges():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://telecom-bucket-135/recharges.csv")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


# =========================
# SUBSCRIPTIONS
# =========================
@dlt.table(name="bronze_subscriptions")
def bronze_subscriptions():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://telecom-bucket-135/subscriptions.csv")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


# =========================
# TOWERS
# =========================
@dlt.table(name="bronze_towers")
def bronze_towers():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://telecom-bucket-135/towers.csv")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


# =========================
# USAGE
# =========================
@dlt.table(name="bronze_usage")
def bronze_usage():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3://telecom-bucket-135/usage.csv")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )