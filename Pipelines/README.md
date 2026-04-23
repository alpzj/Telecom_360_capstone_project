#  Telecom Customer 360 – Pipeline

##  Overview
This project implements a **Telecom Customer 360 analytics platform** using **Databricks Lakehouse architecture (Delta Live Tables - DLT)**.  
It integrates customer, recharge, usage, complaint, and campaign data to generate **business KPIs and insights**.

---

##  Architecture

S3 (Raw CSV Data)
↓
Bronze Layer (Raw Ingestion + Metadata)
↓
Silver Layer (Clean + Transform + Customer 360)
↓
Gold Layer (Business KPIs)
↓
Dashboard (Databricks SQL / BI Tools)

---


---

## 📂 Data Source (AWS S3)

All raw data is stored in:

### Files:
- customers (1).csv  
- subscriptions.csv  
- usage.csv  
- recharges.csv  
- complaints.csv  
- plans.csv  
- campaigns.csv  
- towers.csv  

---

## 🥉 Bronze Layer (Raw Data Ingestion)

### Objective:
- Load raw CSV data from S3  
- Preserve schema  
- Add metadata columns  

### Features:
- No transformations  
- Schema preserved  
- Metadata columns:
  - `load_time`
  - `source_file`

---

## 🥈 Silver Layer (Data Cleaning & Transformation)

### Objective:
- Clean and standardize data  
- Validate records  
- Build unified Customer 360  

### Transformations:
- Remove duplicates  
- Validate `customer_id`  
- Parse date columns  
- Standardize plan names  
- Filter invalid data  

### Tables Created:
- silver_customers  
- silver_plans  
- silver_subscriptions  
- silver_recharges  
- silver_usage  
- silver_complaints  
- silver_campaigns  
- silver_towers  
- silver_customer_360 ⭐  
- silver_monthly_usage  

---

##  Customer 360

Unified view combining:
- Customer details  
- Subscription info  
- Recharge history  
- Usage data  
- Complaints  
- Campaign responses  

### Derived Metric:
- `tenure_days`

---

## 🥇 Gold Layer (Business KPIs)

### Objective:
Create analytics-ready datasets for dashboards.

---

### 📊 KPIs Implemented

#### 1. ARPU (Average Revenue Per User)
- Monthly revenue per user  

#### 2. Churn Risk
- Customers inactive for >30 days  
- Includes `churn_flag`  

#### 3. Recharge Trends
- Monthly revenue & recharge count  

#### 4. Campaign ROI
- Conversion rate  
- Revenue generated  

#### 5. Complaint SLA
- Average resolution time  

#### 6. Region Performance
- Revenue by state  

---

### ⭐ Additional KPIs

#### 7. Plan Performance
- Revenue by plan  

#### 8. Complaint Hotspots
- Complaint count by city  

---

## ⚙️ Technologies Used

- Databricks (DLT Pipelines)  
- PySpark  
- Delta Lake  
- AWS S3  
- SQL  

---

##  Pipeline Design

| Layer  | Purpose                |
|--------|----------------------|
| Bronze | Raw ingestion         |
| Silver | Cleaning & joins      |
| Gold   | Business KPIs         |

---

##  Key Insights

- Identify churn-prone customers  
- Track revenue trends  
- Measure campaign performance  
- Analyze complaints  
- Understand regional performance  

---

##  Data Governance

- Separate schemas:
  - telecom.bronze  
  - telecom.silver  
  - telecom.gold  
- Metadata tracking enabled  
- Structured pipeline design  

---

##  Future Enhancements

- Real-time streaming ingestion  
- Churn prediction (ML model)  
- Dashboard integration (Power BI / Databricks SQL)  
- CI/CD pipeline  

---

##  Conclusion

This project demonstrates a **complete end-to-end data engineering pipeline** with:

- Scalable architecture  
- Clean data modeling  
- Business-driven KPIs  
- Industry best practices  
