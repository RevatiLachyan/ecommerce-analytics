# Brazilian E-Commerce Analytics Pipeline

An end-to-end data analytics project analyzing 100K+ orders from Brazilian e-commerce marketplace data, built with modern data stack technologies.

<img width="2720" height="1535" alt="1 Title Slide" src="https://github.com/user-attachments/assets/0dc1a335-86bc-45a7-84b1-a5537bfa6e73" />

## 🎯 Project Overview

This project demonstrates a production-ready data pipeline that transforms raw e-commerce transaction data into actionable business insights through three interactive dashboards serving different stakeholder needs.

https://public.tableau.com/app/profile/revati.mades/viz/ECommerceAnalytics_17774829454100/E-commerceAnalyticsStory


## 📊 Key Insights

- **Revenue Growth:** 15% year-over-year growth from 2016-2018
- **Geographic Concentration:** São Paulo represents 42% of total orders
- **Delivery Performance:** Northern states experience 2x slower delivery than southern regions
- **Product Analysis:** Health & beauty products drive highest revenue (R$1.4M)
- **Payment Preferences:** 76% of customers prefer credit card payments

## 🛠️ Tech Stack

- **Data Extraction:** Python (Pandas)
- **Data Warehouse:** Snowflake
- **Data Transformation:** dbt (data build tool)
- **Data Visualization:** Tableau Public
- **Version Control:** Git/GitHub

Kaggle Dataset (CSV)
↓
Python Scripts (Data Cleaning)
↓
Snowflake (Cloud Data Warehouse)
├── RAW Schema (7 tables, ~500K rows)
├── STAGING Schema (7 views, cleaned data)
└── ANALYTICS Schema (Star schema: 1 fact + 3 dimension tables)
↓
dbt (SQL Transformations + Data Quality Tests)
↓
Tableau (3 Interactive Dashboards)


## 📂 Data Model

### Star Schema Design

**Fact Table:**
- `fact_orders` - One row per order with metrics (order value, delivery time, ratings)

**Dimension Tables:**
- `dim_customers` - Customer information and segmentation
- `dim_products` - Product catalog and categories  
- `dim_sellers` - Seller locations and performance

## 📊 Dashboards

### 1. Executive Overview
**Audience:** C-level executives, leadership team

**Purpose:** High-level business performance metrics

**Features:**
- Revenue, orders, AOV, and delivery time KPIs
- Monthly revenue trends (2016-2018)
- Order status breakdown (97% delivered)
- Payment method distribution
- Revenue by state (geographic heat map)

<img width="2893" height="1723" alt="2  Executive Dashboard" src="https://github.com/user-attachments/assets/9a5c5f93-6daf-4b54-8ec2-2dd39f749c69" />

---

### 2. Customer & Product Analytics
**Audience:** Marketing team, product managers

**Purpose:** Customer behavior and product performance insights

**Features:**
- Top 10 product categories by revenue
- Top 10 states by order volume
- Customer distribution across Brazil
- Average order value by state (identify premium markets)

<img width="2908" height="1699" alt="3  Customer   Product Analytics" src="https://github.com/user-attachments/assets/04204616-8ed6-463e-92dd-aa397a8a72e0" />

---

### 3. Operations & Delivery Performance
**Audience:** Operations team, logistics managers

**Purpose:** Delivery optimization and bottleneck identification

**Features:**
- Delivery time heat map by state
- Late delivery percentage trend over time
- Delivery speed distribution (Fast/Normal/Slow/Very Slow)
- Top 10 states with most late deliveries

<img width="2903" height="1647" alt="4  Operations   Delivery Performance" src="https://github.com/user-attachments/assets/c09e4fb5-04aa-4fba-84fd-50bb8b5bca16" />


## 🔍 Data Quality

Implemented **15 automated data quality tests** using dbt:

- **Uniqueness Tests:** Primary keys (order_id, customer_id, product_id)
- **Not-Null Tests:** Critical fields (order_value, customer_id, delivery_days)
- **Relationship Tests:** Foreign key integrity (fact → dimension tables)
- **Business Logic Tests:** Order values ≥ 0, delivered orders have items

**Test Pass Rate:** 97%

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Snowflake account (free trial)
- dbt-core and dbt-snowflake
- Tableau Public (free)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/YourUsername/ecommerce-analytics.git
cd ecommerce-analytics
```

2. **Install Python dependencies**
```bash
pip install pandas snowflake-connector-python
```

3. **Install dbt**
```bash
pip install dbt-core dbt-snowflake
```

4. **Download the dataset**
- Source: [Brazilian E-Commerce Dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Extract to `data/` folder

### Setup

1. **Prepare data for Snowflake**
```bash
python scripts/prepare_for_snowflake.py
```

2. **Configure dbt profile**

Create `~/.dbt/profiles.yml`:
```yaml
ecommerce_dbt:
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      database: ECOMMERCE_ANALYTICS
      warehouse: COMPUTE_WH
      schema: ANALYTICS
      threads: 4
  target: dev
```

3. **Load data to Snowflake**
- Create database: `ECOMMERCE_ANALYTICS`
- Create schemas: `RAW`, `STAGING`, `ANALYTICS`
- Load prepared CSVs via Snowflake UI

4. **Run dbt models**
```bash
cd dbt_project
dbt deps  # Install packages
dbt run   # Build models
dbt test  # Run data quality tests
```

5. **Connect Tableau**
- Export final tables as CSV (fact_orders, dim_customers, dim_products, dim_sellers)
- Connect Tableau Public to CSVs
- Build dashboards using the data model


## 💡 Business Impact

This analytics solution enables:

**For Executives:**
- Track overall business health at a glance
- Monitor revenue trends and growth
- Identify operational bottlenecks

**For Marketing:**
- Target high-value customer segments
- Optimize product mix and inventory
- Focus campaigns on profitable regions

**For Operations:**
- Identify delivery performance gaps
- Prioritize warehouse locations
- Reduce late delivery rates

## 🎓 Key Learnings

- **Data Modeling:** Designed star schema optimizing for query performance
- **dbt Best Practices:** Modular SQL, DRY principles, automated testing
- **Data Quality:** Implemented comprehensive testing framework
- **Stakeholder Focus:** Tailored dashboards to different audience needs
- **End-to-End Pipeline:** Managed complete data lifecycle from raw to insights

## 📝 Future Improvements

- [ ] Implement incremental dbt models for performance
- [ ] Add customer lifetime value (CLV) analysis
- [ ] Create predictive model for delivery time estimation
- [ ] Build automated alerting for late delivery spikes
- [ ] Add product recommendation engine

## 📧 Contact
Revati Lachyan Mades
- LinkedIn: https://www.linkedin.com/in/revatilachyan/
- Email: revatilachyanmades@gmail.com

## 📄 License

This project is licensed under the MIT License

## 🙏 Acknowledgments

- Dataset: [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Tools: Snowflake, dbt Labs, Tableau Public
