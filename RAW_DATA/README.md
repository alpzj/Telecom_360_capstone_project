#  Telecom Raw Data – README

##  Overview
This directory contains the **raw telecom datasets** used for the Telecom Customer 360 analytics project.  
The data is stored in **AWS S3** and represents **unprocessed source data** ingested into the Bronze layer of the Lakehouse architecture.

---


##  Dataset Files

### 1. customers.csv
Contains customer demographic details.

| Column Name   | Description |
|--------------|------------|
| customer_id  | Unique customer identifier |
| name         | Customer name |
| gender       | Gender (M/F) |
| age          | Age of customer |
| city         | City |
| state        | State |
| join_date    | Date of joining |
| status       | Active/Inactive |

---

### 2. subscriptions.csv
Customer subscription and plan details.

| Column Name       | Description |
|------------------|------------|
| subscription_id  | Unique subscription ID |
| customer_id      | Customer reference |
| plan_id          | Plan identifier |
| subscription_type| Prepaid/Postpaid |
| start_date       | Subscription start |
| end_date         | Subscription end |

---

### 3. usage.csv
Customer telecom usage data.

| Column Name     | Description |
|----------------|------------|
| usage_id       | Usage record ID |
| customer_id    | Customer reference |
| date           | Usage date |
| data_usage_mb  | Data usage (MB) |
| call_minutes   | Call duration |
| sms_count      | SMS count |

---

### 4. recharges.csv
Recharge transaction data.

| Column Name     | Description |
|----------------|------------|
| recharge_id    | Recharge ID |
| customer_id    | Customer reference |
| recharge_date  | Recharge date |
| amount         | Recharge amount |
| payment_mode   | Payment method |

---

### 5. complaints.csv
Customer complaint records.

| Column Name      | Description |
|-----------------|------------|
| complaint_id    | Complaint ID |
| customer_id     | Customer reference |
| complaint_type  | Type of issue |
| description     | Complaint details |
| status          | Open/Closed |
| created_date    | Complaint date |
| resolved_date   | Resolution date |

---

### 6. plans.csv
Available telecom plans.

| Column Name         | Description |
|--------------------|------------|
| plan_id            | Plan ID |
| plan_name          | Plan name |
| price              | Plan price |
| validity_days      | Validity period |
| data_limit_mb      | Data limit |
| call_limit_minutes | Call limit |

---

### 7. campaigns.csv
Marketing campaign data.

| Column Name         | Description |
|--------------------|------------|
| campaign_id        | Campaign ID |
| customer_id        | Target customer |
| campaign_name      | Campaign name |
| response           | Accepted/Rejected |
| revenue_generated  | Revenue from campaign |
| date               | Campaign date |

---

### 8. towers.csv
Network tower information.

| Column Name     | Description |
|----------------|------------|
| tower_id       | Tower ID |
| location       | Location |
| city           | City |
| state          | State |
| signal_strength| Signal quality |
| status         | Active/Inactive |

---

##  Role in Architecture (Bronze Layer)

- Raw ingestion source  
- No transformations applied  
- Schema preserved  
- Metadata added during ingestion:
  - `load_time`
  - `source_file`

---

##  Notes

- Data is **synthetic (generated for project use)**  
- Used for **analytical and educational purposes only**  
- Relationships maintained via `customer_id` and `plan_id`  

---

## 🔗 Data Relationships

customers ─── subscriptions ─── plans
customers ─── usage
customers ─── recharges
customers ─── complaints
customers ─── campaigns


---

##  Next Step

Raw data is ingested into the **Bronze layer**, then processed into:
- Silver layer (cleaned data)
- Gold layer (business KPIs)

---
