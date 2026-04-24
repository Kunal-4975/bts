# Data Engineer Technical Assessment — dbt + Snowflake

## 1. Overview

This repository is my submission for the Data Engineer technical assessment. The objective of the exercise is to transform operational data that originates in MongoDB (as nested JSON) and lands in Snowflake as `VARIANT` raw tables into analytics-ready fact and dimension tables using **dbt**, so that the data can be consumed in **Power BI**.

The pipeline handles the four challenges called out in the brief:

- Nested and complex JSON structures
- Schema drift over time
- Late-arriving updates
- Soft deletes (via `isDeleted` flag)

---

## 2. Architecture

**End-to-end flow:**

```
MongoDB (regional DBs)  →  Snowflake RAW (VARIANT)  →  dbt (staging → marts)  →  Power BI
```

The architecture diagram is included in the repository as `Architecture.png` and covers:

- MongoDB multi-region sources (e.g., `INWEST`, `EAST`) containing `orders`, `customers`, `products`
- Incremental ingestion into Snowflake raw tables using an `updated_at` watermark with a 3-day lookback to capture late-arriving updates
- The dbt transformation layer (staging + marts) running inside Snowflake
- dbt data quality checks applied at both the staging and marts layers
- Power BI consumption layer with Row-Level Security (RLS) applied on the `region` attribute

---

## 3. Repository Structure

```
bts/
├── Architecture.png              # Architecture diagram (Part 1 deliverable)
├── README.md                     # This file
├── dbt_project.yml               # dbt project configuration
├── profiles.yml                  # Snowflake connection profile (credentials redacted before sharing)
└── models/
    ├── sources.yml               # Raw source definitions with freshness rules
    ├── schema.yml                # Model tests (not_null, unique, relationships)
    ├── staging/
    │   ├── stg_orders.sql
    │   ├── stg_customers.sql
    │   └── stg_products.sql
    └── marts/
        ├── facts/
        │   ├── fct_orders.sql
        │   └── fct_order_items.sql
        └── dimmensions/          # folder name as committed
            ├── dim_customer.sql
            └── dim_product.sql
```

**Materialisation strategy (from `dbt_project.yml`):**

- `staging` → `view`
- `marts` → `table` (with `fct_orders` overridden to `incremental`)

---

## 4. Models

### 4.1 Staging Layer

Staging models unwrap the raw `VARIANT` JSON into flat, typed columns and apply light cleanup.

| Model | Source | Purpose |
|-------|--------|---------|
| `stg_orders` | `raw.orders` | Extracts order-level fields, keeps `statusHistory` and `items` as arrays for downstream flattening, casts `audit.createdAt` / `audit.updatedAt` to timestamps, filters out rows with null `orderId` |
| `stg_customers` | `raw.customers` | Extracts customer fields, flattens the nested name object into `first_name` / `last_name`, picks the first email from the `contacts.emails` array, surfaces `loyalty.tier` |
| `stg_products` | `raw.products` | Extracts product identifiers, SKU, name, two-level category (`l1` / `l2`), and the `active` flag |

### 4.2 Mart Layer — Facts

#### `fct_orders` — 1 row per `order_id`

Materialised as **incremental** with `merge` strategy on `order_id`. Key logic:

- **Deduplication:** `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_updated_at DESC)` keeps the latest version of each order.
- **Incremental filter:** On incremental runs, only pulls rows where `order_updated_at` is greater than the current max in the target table.
- **Soft deletes:** Rows where `is_deleted = true` are filtered out.
- **Current status:** Extracted by flattening `statusHistory` and taking the record with the maximum timestamp per order.
- **Financials:** `gross_amount`, `discount_amount` and `net_amount` are computed by flattening `items` and aggregating at the order grain. Discounts assume the first discount in the array is percentage-based (see Assumptions).

Output columns: `order_id`, `region`, `customer_id`, `order_created_at`, `order_updated_at`, `current_status`, `gross_amount`, `discount_amount`, `net_amount`.

#### `fct_order_items` — 1 row per `order_id` + `item_id`

Materialised as **table**. Flattens the `items` array from `stg_orders` using `LATERAL FLATTEN`. Computes `line_gross`, `line_discount`, and `line_net` at the item level.

Output columns: `order_id`, `region`, `item_id`, `product_id`, `quantity`, `unit_price`, `currency`, `line_gross`, `line_discount`, `line_net`.

### 4.3 Mart Layer — Dimensions

| Model | Purpose |
|-------|---------|
| `dim_customer` | Customer dimension. Concatenates `first_name` + `last_name` into `customer_name`, filters out soft-deleted customers, and derives a simple `customer_segment` (`VIP` for Gold loyalty tier, otherwise `Standard`) |
| `dim_product` | Product dimension. Selects distinct products with SKU, name, two-level category, and active flag |

---

## 5. Key Design Decisions

**Incremental loading.** `fct_orders` uses dbt's incremental materialisation with the `merge` strategy on `order_id`. The filter `order_updated_at > (select coalesce(max(order_updated_at), '1900-01-01') from {{ this }})` ensures only new or updated orders are processed on subsequent runs. The merge key guarantees that late-arriving updates overwrite the previous version of the same order — making the model idempotent.

**Deduplication.** A `ROW_NUMBER()` window function partitioned on `order_id` and ordered by `order_updated_at DESC` ensures exactly one row per order at the fact grain, even when raw has multiple versions of the same order.

**Handling nested JSON.** Snowflake's `LATERAL FLATTEN` is used to unpack `statusHistory` (to derive `current_status`) and `items` (to compute order-level and line-level financials, and to build `fct_order_items`).

**Late-arriving updates.** The combination of (a) incremental `merge` on `order_id` and (b) the `ORDER BY order_updated_at DESC` in the deduplication window means a late update for an existing order will replace the existing row with the latest values on the next run. The architecture diagram shows a 3-day lookback at the ingestion layer as an additional safety net.

**Soft deletes.** The `is_deleted` flag is preserved through staging. Mart models filter it out (`where is_deleted = false`) so that downstream reporting never sees deleted records while the flag remains recoverable from staging if needed.

---

## 6. Data Quality Tests

Tests are declared in `models/schema.yml` and run via `dbt test`:

- **Not-null:** primary keys and critical timestamps across staging, dimensions, and facts (`order_id`, `customer_id`, `product_id`, `order_updated_at`, `region`, `product_name`).
- **Uniqueness:** `order_id` in `fct_orders`; `customer_id` in `dim_customer`; `product_id` in `dim_product`.
- **Referential integrity:** `fct_orders.customer_id` → `dim_customer.customer_id` (dbt `relationships` test).
- **Source freshness:** Configured in `sources.yml` for all three raw tables, using the appropriate `audit.updatedAt` field as the loaded-at field. Thresholds: orders warn at 12h / error at 24h; customers warn at 24h / error at 48h; products warn at 24h / error at 72h.

---

## 7. How to Run

```bash
# 1. Configure Snowflake credentials in profiles.yml (see §10)
dbt debug      # verify connection
dbt run        # build all models (staging → marts)
dbt test       # run all declared tests
dbt source freshness   # check raw source freshness
```

---

## 8. Assumptions

- Discounts are percentage-based (`type = "PCT"`).
- Only the **first** discount per item is applied; multi-discount arrays are a future improvement.
- Source JSON is ingested into Snowflake without upstream transformation.
- `audit.updatedAt` is trustworthy and monotonically non-decreasing per record — incremental logic depends on this.
- The first email in `contacts.emails` is the primary email for the customer dimension.
- All items in the same order share a common currency (currency is carried at the item level, not re-asserted at the order grain).

---

## 9. Short Answers (Assessment Part 3 & beyond)

These are addressed in the accompanying submission document. In summary:

- **Deletes from MongoDB** are captured via the `isDeleted` flag rather than physical deletes, so they propagate as updates; mart models filter them out while staging retains the history.
- **Scale:** the architecture scales horizontally — Snowflake warehouses can be sized per workload, raw tables can be clustered on `region` / `updatedAt`, and dbt can be parallelised on thread count. Incremental models bound per-run compute.
- **Power BI RLS** is best implemented **inside Power BI** using the `region` attribute present on every mart, with RLS roles mapped to user AD groups. The `region` column is available on all facts and dimensions precisely to enable this.
