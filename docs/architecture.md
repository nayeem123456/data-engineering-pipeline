# Architecture Blueprint: Production-Style Data Lakehouse & ETL Platform

## 1. System Overview

The **Production-Style Data Lakehouse & ETL Platform** is an enterprise data engineering solution implementing the **Medallion Architecture (Bronze -> Silver -> Gold)** for high-volume e-commerce transaction and behavioral data. It bridges the gap between raw data lake storage (Parquet files) and operational data warehousing (dimensional star schema in PostgreSQL).

```mermaid
flowchart TD
    subgraph Sources [Source Systems]
        S1[Orders CSV/JSON]
        S2[Customers JSON]
        S3[Products Catalog CSV]
        S4[Payments & Refunds JSON]
        S5[Web Clickstream JSON]
    end

    subgraph Bronze [Bronze Layer - Raw Storage]
        B1[(Raw Partitioned Parquet\ningestion_date=YYYY-MM-DD)]
        B_Meta[Source File Manifests\n& Ingestion Batches]
    end

    subgraph Silver [Silver Layer - Cleansed & Conformed]
        SP[PySpark / Engine Adapter\nSchema Enforcement & Deduplication]
        S_Clean[(Conformed Parquet\nDeterministic Business Keys)]
        Q_Box[(Quarantine Directory / DB\nInvalid Records & Audit Reasons)]
    end

    subgraph Gold [Gold Layer - Dimensional Models & Marts]
        DBT[dbt / SQL Dimensional Modeling]
        DIM[dim_customers, dim_products, dim_date]
        FACT[fact_orders, fact_order_items, fact_payments]
        MART[mart_daily_sales, mart_customer_ltv\nmart_product_performance, mart_refund_analysis]
    end

    subgraph Serving [Analytics & Serving Layer]
        PG[(PostgreSQL Analytics Serving)]
        BI[Streamlit Observability Dashboard & BI]
    end

    subgraph Governance [Governance, Quality & Observability]
        DQ[Multi-Tier Data Quality Gates\nBronze, Silver, Gold Checks]
        META[(PostgreSQL Operational Metadata\nRuns, Batches, Watermarks, Quality)]
        AF[Apache Airflow Orchestrator\nDAGs, Retries, Backfills]
    end

    Sources -->|Ingest & Checksum| B1
    Sources -.->|File Manifest| B_Meta
    B1 --> SP
    SP -->|Valid Records| S_Clean
    SP -->|Invalid / Malformed| Q_Box
    S_Clean --> DBT
    DBT --> DIM & FACT & MART
    DIM & FACT & MART --> PG
    PG --> BI
    
    AF -.->|Orchestrates| Sources
    AF -.->|Orchestrates| SP
    AF -.->|Orchestrates| DBT
    AF -.->|Gate Check| DQ
    DQ -.->|Record Metrics| META
    SP -.->|Log Stats| META
    DBT -.->|Log Stats| META
```

---

## 2. Storage Boundaries & Medallion Data Tiers

### 2.1 Bronze Layer (Raw Ingestion)
- **Physical Location:** `data/lakehouse/bronze/{entity}/ingestion_date=YYYY-MM-DD/{batch_id}_{filename}.parquet`
- **Design Philosophy:** Preserves raw payloads exactly as emitted by upstream systems. No destructive transformations or schema coercion.
- **Audit Lineage Columns Attached:**
  - `_ingestion_id`: Globally unique UUID identifying the ingestion event.
  - `_batch_id`: Batch correlation identifier.
  - `_source_name`: Ingesting source subsystem.
  - `_source_file`: Original filename.
  - `_source_file_checksum`: SHA-256 cryptographic digest of the source file to prevent duplicate file ingestion.
  - `_ingested_at`: UTC ISO timestamp when file was ingested.
  - `_source_record_id`: Natural identifier or file offset.

### 2.2 Silver Layer (Cleansed, Conformed, Deduplicated)
- **Physical Location:** `data/lakehouse/silver/{entity}/data.parquet`
- **Cleansing & Conforming Rules:**
  - Strict typing: UTC Timestamps, standardized currencies (ISO-4217), two-decimal precision monetary values (`DECIMAL(12, 2)` / `FLOAT64`).
  - Column name standardization: strictly lower snake_case.
  - Deterministic Business Key Deduplication: Partition by business key (`order_id`, `customer_id`, etc.), order by `updated_at DESC, _ingested_at DESC`, retaining rank 1.
  - Quarantine Isolation: Rows violating domain constraints (negative prices, null keys, invalid status codes) are routed to `data/lakehouse/quarantine/` and `metadata.quarantined_records`.
  - Schema Evolution: Tracks schema change events, accepting compatible new optional columns while quarantining or failing breaking changes.

### 2.3 Gold Layer (Dimensional Models & Analytical Marts)
- **Physical Location:** `data/lakehouse/gold/{table_name}/data.parquet` and PostgreSQL schema `gold`
- **Dimensional Modeling:**
  - Kimball Star Schema:
    - Dimensions: `dim_date`, `dim_customers`, `dim_products` (Surrogate keys: `date_key`, `customer_sk`, `product_sk`).
    - Facts: `fact_orders`, `fact_order_items`, `fact_payments` (Order grain, line item grain, payment grain).
  - Business Marts:
    - `mart_daily_sales`: Daily executive summary and margin reconciliation.
    - `mart_customer_lifetime_value`: Customer RFM and tenure segmentation.
    - `mart_product_performance`: Product-level margin percentage and return rates.
    - `mart_refund_analysis`: Payment method dispute and refund analysis.

---

## 3. Incremental Processing, Watermarks & Idempotency

### 3.1 Watermark Persistence
Incremental processing relies on persistent high watermarks stored in `metadata.watermarks`:
- `entity_name`: Target entity (`orders`, `customers`, etc.)
- `watermark_type`: `TIMESTAMP`
- `watermark_value`: ISO 8601 UTC timestamp of latest processed source data
- `last_batch_id`: Batch identifier advancing the watermark
- `updated_at`: Timestamp of checkpoint update

### 3.2 Overlapping Lookback Windows (Late-Arriving Events)
Upstream systems frequently generate delayed updates (e.g. status changes from `PROCESSING` to `SHIPPED` hours or days later).
To capture these updates without reprocessing the entire history, the platform computes an extraction window:
$$\text{Window Start} = \text{Watermark} - \text{Grace Period (e.g., 24 hours)}$$
$$\text{Window End} = \text{Current Execution Timestamp}$$

### 3.3 Safe Staging & Publish (Idempotency Guarantee)
Re-running a batch or pipeline run must never duplicate records or inflate metrics:
1. **Silver Parquet:** Staged write + deterministic window deduplication on `(business_key, updated_at DESC)`.
2. **PostgreSQL Serving:** `INSERT INTO gold... ON CONFLICT (order_sk) DO UPDATE...`
3. **Automated Testing:** `test_pipeline_idempotency_on_repeated_runs` verifies that processing the exact same batch produces identical row counts, identical gross revenue, and zero duplicate keys.

---

## 4. Automated Data Quality Barrier (Quality Gates)

The platform enforces automated quality checks at each medallion tier. If critical checks fail, the pipeline halts immediately, preventing invalid data from corrupting downstream Gold marts.

```mermaid
graph TD
    A[Bronze Data Ingested] --> B{Bronze Quality Gate}
    B -- Pass --> C[Silver Processing & Quarantine]
    B -- Fail: Corrupt/Empty --> H[Halt Pipeline & Alert]
    C --> D{Silver Quality Gate}
    D -- Pass: Unique Keys, Non-Null --> E[Build Gold dbt Models]
    D -- Fail: Critical Rule Violation --> H
    E --> F{Gold Reconciliation Gate}
    F -- Pass: Revenue Reconciled --> G[Publish to PostgreSQL Serving & Advance Watermark]
    F -- Fail: Discrepancy > 0.05 --> H
```

- **Bronze Checks:** File readability, Parquet format validation, non-empty row count, mandatory audit columns (`_ingestion_id`, `_batch_id`, `_source_file_checksum`).
- **Silver Checks:** Primary key uniqueness, non-null mandatory fields, non-negative monetary amounts, valid categorical statuses.
- **Gold Checks:** Surrogate key uniqueness in dimensions, fact-to-dimension referential integrity, financial revenue reconciliation ($\sum \text{fact\_orders.total\_amount} == \sum \text{mart\_daily\_sales.gross\_revenue}$).
