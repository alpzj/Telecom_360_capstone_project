#1. ARPU (Average Revenue Per User)
import dlt
from pyspark.sql.functions import *

@dlt.table(name="gold_arpu")
def gold_arpu():
    df = dlt.read("silver_recharges")
    return df.groupBy(
        date_format("recharge_date", "yyyy-MM").alias("month")
    ).agg(
        (sum("amount") / countDistinct("customer_id")).alias("arpu")
    )
#2. CHURN RISK
@dlt.table(name="gold_churn_risk")
def gold_churn_risk():

    df = dlt.read("silver_recharges")

    return df.groupBy("customer_id").agg(
        max("recharge_date").alias("last_recharge")
    ).withColumn(
        "days_inactive",
        datediff(current_date(), col("last_recharge"))
    ).withColumn(
        "churn_flag",
        when(col("days_inactive") > 30, 1).otherwise(0)
    )
#3. RECHARGE TRENDS
@dlt.table(name="gold_recharge_trends")
def gold_recharge_trends():

    df = dlt.read("silver_recharges")

    return df.groupBy(
        date_format("recharge_date", "yyyy-MM").alias("month")
    ).agg(
        sum("amount").alias("total_revenue"),
        count("*").alias("total_recharges")
    )
#4. CAMPAIGN ROI
@dlt.table(name="gold_campaign_roi")
def gold_campaign_roi():

    df = dlt.read("silver_campaigns")

    return df.groupBy("campaign_name").agg(
        count("*").alias("targeted"),
        sum(when(col("response") == "Accepted", 1).otherwise(0)).alias("converted"),
        sum("revenue_generated").alias("revenue")
    ).withColumn(
        "conversion_rate",
        col("converted") / col("targeted")
    )
#5. COMPLAINT SLA
@dlt.table(name="gold_complaint_sla")
def gold_complaint_sla():

    df = dlt.read("silver_complaints")

    return df.withColumn(
        "resolution_days",
        datediff(col("resolved_date"), col("created_date"))
    ).groupBy("complaint_type").agg(
        avg("resolution_days").alias("avg_resolution_days")
    )
#6. REGION PERFORMANCE
@dlt.table(name="gold_region_performance")
def gold_region_performance():

    customers = dlt.read("silver_customers").select(
        "customer_id", "state"
    )

    recharges = dlt.read("silver_recharges").select(
        "customer_id", "amount"
    )

    return customers.join(recharges, "customer_id") \
        .groupBy("state") \
        .agg(
            sum("amount").alias("total_revenue"),
            countDistinct("customer_id").alias("customers")
        )
#7. TOP PLANS PERFORMANCE
@dlt.table(name="gold_plan_performance")
def gold_plan_performance():

    subs = dlt.read("silver_subscriptions").select(
        "customer_id", "plan_name"
    )

    recharges = dlt.read("silver_recharges").select(
        "customer_id", "amount"
    )

    return subs.join(recharges, "customer_id") \
        .groupBy("plan_name") \
        .agg(sum("amount").alias("revenue"))
#8. COMPLAINT HOTSPOTS
@dlt.table(name="gold_complaint_hotspots")
def gold_complaint_hotspots():

    customers = dlt.read("silver_customers").select(
        "customer_id", "city"
    )

    complaints = dlt.read("silver_complaints")

    return customers.join(complaints, "customer_id") \
        .groupBy("city") \
        .agg(count("*").alias("total_complaints"))