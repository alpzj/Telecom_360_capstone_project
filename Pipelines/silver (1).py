import dlt
from pyspark.sql.functions import *

# =========================
# CUSTOMERS
# =========================
@dlt.table(name="silver_customers")
def silver_customers():
    return (
        dlt.read("bronze_customers")
        .dropDuplicates(["customer_id"])
        .filter(col("customer_id").isNotNull())
        .withColumn("join_date", to_date("join_date"))
    )

# =========================
# PLANS
# =========================
@dlt.table(name="silver_plans")
def silver_plans():
    return (
        dlt.read("bronze_plans")
        .withColumn("plan_name", upper(trim(col("plan_name"))))
    )

# =========================
# SUBSCRIPTIONS
# =========================
@dlt.table(name="silver_subscriptions")
def silver_subscriptions():

    subs = dlt.read("bronze_subscriptions").select(
        "subscription_id",
        "customer_id",
        "plan_id",
        "subscription_type",
        "start_date",
        "end_date"
    )

    plans = dlt.read("silver_plans").select(
        "plan_id",
        "plan_name",
        "price",
        "validity_days"
    )

    return subs.join(plans, "plan_id", "left")

# =========================
# RECHARGES
# =========================
@dlt.table(name="silver_recharges")
def silver_recharges():
    return (
        dlt.read("bronze_recharges")
        .filter(col("amount") > 0)
        .withColumn("recharge_date", to_date("recharge_date"))
    )

# =========================
# USAGE
# =========================
@dlt.table(name="silver_usage")
def silver_usage():
    return (
        dlt.read("bronze_usage")
        .withColumn("date", to_date("date"))
    )

# =========================
# COMPLAINTS
# =========================
@dlt.table(name="silver_complaints")
def silver_complaints():
    return (
        dlt.read("bronze_complaints")
        .withColumn("created_date", to_date("created_date"))
        .withColumn("resolved_date", to_date("resolved_date"))
    )

# =========================
# CAMPAIGNS
# =========================
@dlt.table(name="silver_campaigns")
def silver_campaigns():
    return dlt.read("bronze_campaigns")

# =========================
# TOWERS
# =========================
@dlt.table(name="silver_towers")
def silver_towers():
    return dlt.read("bronze_towers")

# =========================
# VALID CUSTOMERS
# =========================
@dlt.table(name="silver_valid_customers")
def silver_valid_customers():
    return dlt.read("silver_customers") \
        .filter(col("customer_id").rlike("^CUST[0-9]+"))

# =========================
# CUSTOMER 360 (CORE)
# =========================
@dlt.table(name="silver_customer_360")
def silver_customer_360():

    customers = dlt.read("silver_valid_customers").select(
        "customer_id", "name", "gender", "age", "city", "state", "join_date"
    )

    subs = dlt.read("silver_subscriptions").select(
        "customer_id", "plan_id", "plan_name"
    )

    recharges = dlt.read("silver_recharges").select(
        "customer_id", "recharge_date", "amount"
    )

    usage = dlt.read("silver_usage").select(
        "customer_id", "date", "data_usage_mb", "call_minutes", "sms_count"
    )

    complaints = dlt.read("silver_complaints").select(
        "customer_id", "complaint_type", "status"
    )

    campaigns = dlt.read("silver_campaigns").select(
        "customer_id", "campaign_name", "response"
    )

    return customers \
        .join(subs, "customer_id", "left") \
        .join(recharges, "customer_id", "left") \
        .join(usage, "customer_id", "left") \
        .join(complaints, "customer_id", "left") \
        .join(campaigns, "customer_id", "left") \
        .withColumn("tenure_days", datediff(current_date(), col("join_date")))

# =========================
# MONTHLY USAGE
# =========================
@dlt.table(name="silver_monthly_usage")
def silver_monthly_usage():
    usage = dlt.read("silver_usage")

    return usage.groupBy(
        "customer_id",
        date_format("date", "yyyy-MM").alias("month")
    ).agg(
        sum("data_usage_mb").alias("total_data_mb"),
        sum("call_minutes").alias("total_call_minutes"),
        sum("sms_count").alias("total_sms")
    )