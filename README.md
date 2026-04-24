# Data Engineer Assignment – dbt + Snowflake

## 📌 Overview

This project transforms raw JSON data from MongoDB into analytics-ready tables using dbt and Snowflake. The goal is to model clean, reliable fact and dimension tables for downstream reporting (Power BI).

---

## 🏗️ Architecture

**Flow:**

MongoDB (regional DBs) → Snowflake RAW (VARIANT) → dbt (staging → marts) → Power BI

### Key Concepts:

* Raw data is stored as JSON (VARIANT) in Snowflake
* dbt is used for transformations and modeling
* Final tables are optimized for analytics

---

## 📂 Project Structure

```
models/
  staging/
    stg_orders.sql
    stg_customers.sql
    stg_products.sql

  marts/
    facts/
      fct_orders.sql
      fct_order_items.sql

    dimensions/
      dim_customer.sql
      dim_product.sql

models/schema.yml
models/sources.yml
```

---

## 📊 Models

### 🔹 Staging

* `stg_orders` – Extracts and flattens order JSON
* `stg_customers` – Extracts customer details
* `stg_products` – Extracts product attributes

---

### 🔹 Fact Tables

#### `fct_orders`

Grain: **1 row per order_id**

Includes:

* order details
* latest status from status history
* aggregated financial metrics

#### `fct_order_items`

Grain: **1 row per order_id + item_id**

Includes:

* product-level breakdown
* pricing and discount calculations

---

### 🔹 Dimension Tables

* `dim_customer` – customer attributes
* `dim_product` – product attributes

---

## ⚙️ Key Design Decisions

### ✅ Incremental Loading

* `fct_orders` is built incrementally
* Uses `order_updated_at` to process only new/updated records

---

### ✅ Deduplication

* Uses `row_number()` window function
* Keeps latest record per `order_id`

---

### ✅ Handling Nested JSON

* Used `lateral flatten` for:

  * `order_items`
  * `status_history`

---

### ✅ Late Arriving Data

* Always selects latest `order_updated_at`
* Ensures correctness even with delayed updates

---

### ✅ Soft Deletes

* `is_deleted` flag retained and propagated
* Allows filtering at reporting layer

---

## 🧪 Data Quality Tests

Implemented using dbt:

* **Uniqueness**

  * `order_id` in `fct_orders`

* **Not Null**

  * primary keys across models

* **Referential Integrity**

  * `customer_id` → `dim_customer`

* **Freshness Check**

  * Ensures recent data in `fct_orders`

---

## 🚀 How to Run

```bash
dbt debug     # check connection
dbt run       # build models
dbt test      # run tests
```

---

## ⚠️ Assumptions

* Discounts are percentage-based
* Only first discount per item is applied
* Source data is ingested into Snowflake without transformation
* `order_updated_at` is reliable for incremental logic

---

## 📈 Future Improvements

* Handle multiple discounts using array flattening
* Implement near real-time ingestion using Snowflake streams/tasks
* Add clustering for large tables
* Implement row-level security (RLS) using region
* Add data freshness alerts

---

## ✅ Summary

This project demonstrates:

* Handling complex nested JSON data
* Building scalable dbt models
* Implementing incremental pipelines
* Applying data quality checks
* Designing analytics-ready data models

---
