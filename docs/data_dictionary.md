# Data Dictionary & Schema Catalog: E-Commerce Lakehouse

This document provides a comprehensive catalog of all data entities across the Medallion Architecture (Bronze, Silver, Gold) and the Operational Governance Schema.

---

## 1. Bronze Layer (Raw Storage)

Raw files are stored in partitioned columnar Parquet files partitioned by `ingestion_date=YYYY-MM-DD`. Every Bronze record includes four immutable audit columns appended at ingestion.

### Universal Bronze Audit Columns
| Column Name | Data Type | Nullable | Description |
|:---|:---|:---|:---|
| `_ingestion_timestamp` | `TIMESTAMP` | NO | UTC timestamp when the batch was ingested into Bronze. |
| `_source_file` | `STRING` | NO | Original filename or path of the ingested file. |
| `_batch_id` | `STRING` | NO | UUID identifying the specific ingestion execution batch. |
| `_record_hash` | `STRING` | NO | SHA-256 hash of the raw payload for data integrity auditing. |

---

## 2. Silver Layer (Cleaned & Conformed)

Silver datasets are conformed Parquet tables stored at `data/lakehouse/silver/<entity>/`. Business rules, deduplication, type casting, and schema conformance are applied. Malformed records are diverted to the Quarantine store.

### 2.1 `silver.orders`
- **Grain:** One record per business order.
- **Deduplication Strategy:** Partitioned by `order_id`, ordered by `order_timestamp DESC`, taking the latest event.

| Column Name | Type | Nullable | Primary Key | Description |
|:---|:---|:---|:---|:---|
| `order_id` | `STRING` | NO | YES | Unique business identifier for the order (e.g. `ORD-100234`). |
| `customer_id` | `STRING` | NO | NO | Foreign key referencing `silver.customers.customer_id`. |
| `order_status` | `STRING` | NO | NO | Standardized status: `PENDING`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`. |
| `order_timestamp` | `TIMESTAMP` | NO | NO | UTC datetime when order was placed. |
| `currency` | `STRING` | NO | NO | ISO 4217 3-letter currency code (e.g. `USD`, `EUR`). |
| `total_amount` | `DOUBLE` | NO | NO | Total monetary order value inclusive of tax and shipping. Must be >= 0. |
| `shipping_address` | `STRING` | YES | NO | Standardized delivery shipping address. |
| `payment_method` | `STRING` | YES | NO | Payment method used: `CREDIT_CARD`, `PAYPAL`, `APPLE_PAY`, etc. |
| `ingestion_date` | `STRING` | NO | NO | Ingestion partition date (`YYYY-MM-DD`). |
| `_silver_processed_at` | `TIMESTAMP` | NO | NO | UTC timestamp when record was conformed in Silver. |

### 2.2 `silver.order_items`
- **Grain:** One record per line item within an order.
- **Deduplication Strategy:** Deduplicated on `item_id`.

| Column Name | Type | Nullable | Primary Key | Description |
|:---|:---|:---|:---|:---|
| `item_id` | `STRING` | NO | YES | Unique item line-item ID (e.g. `ITM-50012`). |
| `order_id` | `STRING` | NO | NO | Foreign key referencing `silver.orders.order_id`. |
| `product_id` | `STRING` | NO | NO | Foreign key referencing `silver.products.product_id`. |
| `quantity` | `INTEGER` | NO | NO | Number of units purchased (must be > 0). |
| `unit_price` | `DOUBLE` | NO | NO | Unit price in order currency. Must be >= 0. |
| `line_total` | `DOUBLE` | NO | NO | Calculated as `quantity * unit_price`. |
| `ingestion_date` | `STRING` | NO | NO | Ingestion partition date (`YYYY-MM-DD`). |
| `_silver_processed_at` | `TIMESTAMP` | NO | NO | UTC timestamp when conformed. |

### 2.3 `silver.customers`
- **Grain:** One record per registered customer profile.
- **Deduplication Strategy:** Deduplicated on `customer_id`, picking most recent `updated_at`.

| Column Name | Type | Nullable | Primary Key | Description |
|:---|:---|:---|:---|:---|
| `customer_id` | `STRING` | NO | YES | Unique customer identifier (e.g. `CUST-9821`). |
| `email` | `STRING` | NO | NO | Customer email (cleaned, lowercased, trimmed). |
| `first_name` | `STRING` | YES | NO | Customer first name. |
| `last_name` | `STRING` | YES | NO | Customer last name. |
| `customer_segment` | `STRING` | NO | NO | Segment classification: `NEW`, `REGULAR`, `VIP`, `CHURN_RISK`. |
| `country` | `STRING` | YES | NO | 2-character ISO country code. |
| `created_at` | `TIMESTAMP` | NO | NO | Account registration timestamp. |
| `updated_at` | `TIMESTAMP` | NO | NO | Profile last update timestamp. |
| `_silver_processed_at` | `TIMESTAMP` | NO | NO | Conformance audit timestamp. |

### 2.4 `silver.products`
- **Grain:** One record per catalog product SKU.
- **Deduplication Strategy:** Deduplicated on `product_id`, taking latest update.

| Column Name | Type | Nullable | Primary Key | Description |
|:---|:---|:---|:---|:---|
| `product_id` | `STRING` | NO | YES | SKU identifier (e.g. `PROD-0045`). |
| `product_name` | `STRING` | NO | NO | Product title/name. |
| `category` | `STRING` | NO | NO | Product category: `Electronics`, `Apparel`, `Home`, `Beauty`, `Books`. |
| `price` | `DOUBLE` | NO | NO | Current listing price (must be >= 0). |
| `cost` | `DOUBLE` | NO | NO | Unit procurement cost (for margin calculations). |
| `is_active` | `BOOLEAN` | NO | NO | Whether product is currently available for purchase. |
| `created_at` | `TIMESTAMP` | NO | NO | SKU creation timestamp. |
| `_silver_processed_at` | `TIMESTAMP` | NO | NO | Conformance audit timestamp. |

### 2.5 `silver.payments`
- **Grain:** One record per transaction event (charge, refund, chargeback).
- **Deduplication Strategy:** Deduplicated on `payment_id`.

| Column Name | Type | Nullable | Primary Key | Description |
|:---|:---|:---|:---|:---|
| `payment_id` | `STRING` | NO | YES | Unique payment transaction ID (e.g. `PAY-77123`). |
| `order_id` | `STRING` | NO | NO | Foreign key referencing `silver.orders.order_id`. |
| `payment_status` | `STRING` | NO | NO | `COMPLETED`, `PENDING`, `FAILED`, `REFUNDED`. |
| `payment_method` | `STRING` | NO | NO | Payment processor/channel. |
| `amount` | `DOUBLE` | NO | NO | Gross transaction amount. |
| `refund_amount` | `DOUBLE` | NO | NO | Amount refunded (default 0.0). |
| `payment_timestamp` | `TIMESTAMP` | NO | NO | Timestamp transaction was settled. |
| `_silver_processed_at` | `TIMESTAMP` | NO | NO | Conformance audit timestamp. |

### 2.6 `silver.clickstream`
- **Grain:** One record per digital user action (session interaction).
- **Deduplication Strategy:** Deduplicated on `event_id`.

| Column Name | Type | Nullable | Primary Key | Description |
|:---|:---|:---|:---|:---|
| `event_id` | `STRING` | NO | YES | UUID for the event. |
| `session_id` | `STRING` | NO | NO | Web/App browsing session ID. |
| `customer_id` | `STRING` | YES | NO | Nullable user ID (null for guest visitors). |
| `event_type` | `STRING` | NO | NO | `page_view`, `product_view`, `add_to_cart`, `checkout_start`, `purchase`. |
| `page_url` | `STRING` | YES | NO | Visited URI path. |
| `device_type` | `STRING` | YES | NO | `desktop`, `mobile`, `tablet`. |
| `event_timestamp` | `TIMESTAMP` | NO | NO | Event occurrence timestamp. |
| `_silver_processed_at` | `TIMESTAMP` | NO | NO | Conformance audit timestamp. |

---

## 3. Gold Layer (Dimensional Models & Analytics Marts)

Gold layer models follow Kimball dimensional modeling principles. Tables are published to PostgreSQL (and Parquet lakehouse storage) for downstream BI, ad-hoc SQL, and reporting.

### 3.1 Dimensions

#### `dim_date`
- **Grain:** One record per calendar day (covers range 2023-01-01 through 2026-12-31).
- **Primary Key:** `date_id` (Integer integer surrogate: `YYYYMMDD`).
- **Columns:** `date_id`, `calendar_date` (DATE), `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week`, `day_name`, `is_weekend`, `fiscal_quarter`.

#### `dim_customers`
- **Grain:** One record per unique customer (Type 1 SCD with behavioral metrics).
- **Primary Key:** `customer_key` (Deterministic integer/hash surrogate key).
- **Natural Key:** `customer_id`.
- **Columns:** `customer_key`, `customer_id`, `email`, `full_name`, `customer_segment`, `country`, `member_since_date_id`, `lifetime_orders`, `lifetime_spend`, `last_order_timestamp`, `is_active_customer`.

#### `dim_products`
- **Grain:** One record per unique product SKU.
- **Primary Key:** `product_key` (Deterministic surrogate key).
- **Natural Key:** `product_id`.
- **Columns:** `product_key`, `product_id`, `product_name`, `category`, `price`, `cost`, `margin_amount` (`price - cost`), `margin_pct`, `is_active`.

### 3.2 Fact Tables

#### `fact_orders`
- **Grain:** One record per order placed.
- **Primary Key:** `order_key`.
- **Foreign Keys:** `order_date_id` -> `dim_date.date_id`, `customer_key` -> `dim_customers.customer_key`.
- **Metrics:** `gross_amount`, `discount_amount`, `tax_amount`, `shipping_amount`, `net_amount`, `item_count`.
- **Attributes:** `order_id`, `order_status`, `payment_method`, `order_timestamp`.

#### `fact_order_items`
- **Grain:** One record per line item ordered.
- **Primary Key:** `order_item_key`.
- **Foreign Keys:** `order_key` -> `fact_orders.order_key`, `product_key` -> `dim_products.product_key`, `customer_key` -> `dim_customers.customer_key`, `date_id` -> `dim_date.date_id`.
- **Metrics:** `quantity`, `unit_price`, `unit_cost`, `line_gross_revenue`, `line_cost`, `line_margin`.

#### `fact_payments`
- **Grain:** One record per settlement transaction.
- **Primary Key:** `payment_key`.
- **Foreign Keys:** `order_key` -> `fact_orders.order_key`, `payment_date_id` -> `dim_date.date_id`.
- **Metrics:** `payment_amount`, `refund_amount`, `net_collected_amount`.
- **Attributes:** `payment_id`, `order_id`, `payment_status`, `payment_method`.

### 3.3 Analytical Marts (Aggregations)

#### `mart_daily_sales`
- **Grain:** One record per calendar day.
- **Primary Key:** `sales_date`.
- **Metrics:**
  - `total_orders`: Count of completed orders.
  - `total_customers`: Count of unique purchasing customers.
  - `gross_revenue`: Sum of `total_amount` across completed orders.
  - `avg_order_value`: `gross_revenue / total_orders`.
  - `cancelled_orders`: Count of cancelled orders.
  - `shipped_orders`: Count of shipped/delivered orders.

#### `mart_customer_lifetime_value`
- **Grain:** One record per customer.
- **Primary Key:** `customer_id`.
- **Metrics:**
  - `first_order_date`: Date of acquisition order.
  - `most_recent_order_date`: Date of latest purchase.
  - `total_orders`: Cumulative completed order count.
  - `lifetime_spend`: Total cumulative gross revenue generated.
  - `avg_basket_value`: Average spend per order.
  - `clv_segment`: `VIP` (Spend > $1,000), `HIGH_VALUE` ($500-$1000), `STANDARD` (< $500).

#### `mart_product_performance`
- **Grain:** One record per product SKU.
- **Primary Key:** `product_id`.
- **Metrics:**
  - `units_sold`: Total quantity purchased.
  - `order_count`: Distinct orders featuring the product.
  - `gross_sales`: Total gross sales revenue generated.
  - `total_margin`: Total gross margin (`gross_sales - total_cost`).
  - `avg_selling_price`: Weighted average realized price.

#### `mart_refund_analysis`
- **Grain:** One record per calendar day.
- **Primary Key:** `refund_date`.
- **Metrics:**
  - `total_refund_events`: Count of refund transactions.
  - `total_refunded_amount`: Sum of dollars refunded.
  - `gross_settled_amount`: Total gross payments received on that day.
  - `refund_rate_pct`: `(total_refunded_amount / gross_settled_amount) * 100`.

---

## 4. Operational Governance & Metadata Schema

These tables track execution telemetry, lineage, quality gate compliance, and quarantine isolation.

| Table Name | Grain | Purpose | Key Columns |
|:---|:---|:---|:---|
| `metadata.pipeline_runs` | 1 row per pipeline run | Tracks overall workflow status, runtime, and triggering user/schedule. | `run_id`, `pipeline_name`, `status`, `start_time`, `end_time` |
| `metadata.task_runs` | 1 row per task in a run | Tracks execution of individual stages (ingestion, silver, gold, quality). | `task_run_id`, `run_id`, `task_name`, `status`, `duration_seconds` |
| `metadata.ingestion_batches` | 1 row per entity batch | Records input row counts, rejected rows, output rows per batch. | `batch_id`, `entity_name`, `source_type`, `rows_read`, `rows_written` |
| `metadata.file_manifests` | 1 row per ingested file | Prevents duplicate file ingestion via SHA-256 fingerprinting. | `file_path`, `file_hash`, `file_size_bytes`, `status` |
| `metadata.watermarks` | 1 row per entity state | Stores incremental high-watermarks for delta ingestion. | `entity_name`, `watermark_column`, `watermark_value`, `updated_at` |
| `metadata.data_quality_results` | 1 row per validation check | Audit log of all automated data quality assertions across all layers. | `check_name`, `dataset_name`, `layer`, `passed`, `severity`, `metric_value` |
| `metadata.quarantined_records` | 1 row per rejected record | Audits why rows were dropped from Silver/Gold with raw JSON payload. | `quarantine_id`, `entity_name`, `failure_reason`, `raw_record` |
| `metadata.schema_change_events` | 1 row per schema delta | Tracks drift (added columns, dropped columns, type mismatches). | `entity_name`, `change_type`, `column_name`, `is_breaking` |
| `metadata.backfill_runs` | 1 row per backfill job | Records start/end range, partitions reprocessed, and status. | `backfill_id`, `start_date`, `end_date`, `affected_partitions`, `status` |
| `metadata.published_dataset_versions` | 1 row per Gold publishing | Tracks verified snapshot versions exposed to business consumers. | `version_id`, `dataset_name`, `record_count`, `checksum`, `published_at` |
