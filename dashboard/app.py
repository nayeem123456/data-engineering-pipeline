"""
Lakehouse Observability and Operational Monitoring Dashboard.
Real-time Streamlit dashboard reading live operational metadata,
data quality scorecards, quarantined records, and analytical Gold marts.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.common.config import config
from src.common.db import db_manager
from src.orchestration.pipeline_runner import LakehousePipelineRunner

st.set_page_config(
    page_title="Lakehouse Platform Observability",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for modern styling
st.markdown(
    """
<style>
    .main-header { font-size: 2.2rem; font-weight: 700; color: #1E293B; margin-bottom: 0.2rem; }
    .sub-header { font-size: 1.05rem; color: #64748B; margin-bottom: 1.5rem; }
    .metric-card { background: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 8px; padding: 16px; }
    .status-badge-success { background: #DCFCE7; color: #166534; padding: 4px 10px; border-radius: 12px; font-weight: 600; }
    .status-badge-failed { background: #FEE2E2; color: #991B1B; padding: 4px 10px; border-radius: 12px; font-weight: 600; }
</style>
""",
    unsafe_allow_html=True,
)


def get_db_data(query_str: str) -> pd.DataFrame:
    """Execute read query against operational or serving database."""
    try:
        engine = db_manager.get_engine()
        with engine.connect() as conn:
            return pd.read_sql(text(query_str), conn)
    except Exception as exc:
        st.warning(f"Metadata query notice: {exc}")
        return pd.DataFrame()


def render_sidebar():
    st.sidebar.title("🌊 Lakehouse Control")
    st.sidebar.markdown(f"**Environment:** `{config.ENVIRONMENT}`")
    st.sidebar.markdown(
        f"**Store Type:** `{'SQLite (Local)' if db_manager.is_sqlite() else 'PostgreSQL'}`"
    )
    st.sidebar.divider()

    st.sidebar.subheader("🚀 Manual Trigger")
    batch_type = st.sidebar.selectbox("Batch Type", ["INCREMENTAL", "INITIAL", "RELOAD"])
    order_count = st.sidebar.slider("Sample Order Count", 50, 500, 150)
    inject_invalid = st.sidebar.checkbox("Inject Invalid Records", value=False)
    inject_dups = st.sidebar.checkbox("Inject Duplicate Records", value=False)

    if st.sidebar.button("Run Pipeline Now", type="primary"):
        with st.spinner("Executing Lakehouse ETL/ELT Pipeline..."):
            try:
                runner = LakehousePipelineRunner()
                metrics = runner.run_pipeline(
                    batch_type=batch_type,
                    sample_order_count=order_count,
                    inject_invalid=inject_invalid,
                    inject_duplicates=inject_dups,
                )
                st.sidebar.success(f"Pipeline Succeeded! Ingested {metrics['bronze_rows']} rows.")
            except Exception as e:
                st.sidebar.error(f"Pipeline Failed: {e}")

    st.sidebar.divider()
    st.sidebar.subheader("🔄 Backfill Controls")
    bf_start = st.sidebar.text_input("Start Date", value="2026-01-01", key="bf_start")
    bf_end = st.sidebar.text_input("End Date", value="2026-01-07", key="bf_end")
    bf_entities = st.sidebar.multiselect(
        "Target Entities",
        ["orders", "order_items", "customers", "products", "payments"],
        default=["orders", "order_items", "customers", "products", "payments"],
        key="bf_entities",
    )

    if st.sidebar.button("Trigger Historical Backfill", key="btn_backfill"):
        with st.spinner(f"Executing historical backfill [{bf_start} to {bf_end}]..."):
            try:
                from src.orchestration.backfill_runner import BackfillCoordinator

                bf_res = BackfillCoordinator.execute_backfill(
                    start_date=bf_start,
                    end_date=bf_end,
                    entities=bf_entities,
                )
                st.sidebar.success(f"Backfill {bf_res['backfill_id']} completed successfully!")
            except Exception as e:
                st.sidebar.error(f"Backfill Failed: {e}")

    st.sidebar.divider()
    st.sidebar.info(
        "Medallion Architecture: Bronze (Raw Parquet) -> Silver (Conformed) -> Gold (Marts)"
    )


def main():
    render_sidebar()

    st.markdown(
        '<div class="main-header">Production Lakehouse Observability Platform</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="sub-header">Live Telemetry, Data Quality Gates, Quarantine Inspector & Analytics</div>',
        unsafe_allow_html=True,
    )

    tab_overview, tab_runs, tab_dq, tab_quarantine, tab_drift, tab_analytics = st.tabs(
        [
            "📊 Platform Overview",
            "⏱️ Pipeline Runs & Latency",
            "🛡️ Data Quality Scorecard",
            "🚨 Quarantine Inspector",
            "🔍 Schema Drift & Governance",
            "📈 Gold Analytical Marts",
        ]
    )

    prefix = "metadata_" if db_manager.is_sqlite() else "metadata."
    gold_prefix = "" if db_manager.is_sqlite() else "gold."

    # --------------------------------------------------------------------------
    # TAB 1: Platform Overview
    # --------------------------------------------------------------------------
    with tab_overview:
        df_runs = get_db_data(f"SELECT * FROM {prefix}pipeline_runs ORDER BY start_time DESC")
        df_quarantine = get_db_data(f"SELECT COUNT(*) AS cnt FROM {prefix}quarantined_records")
        get_db_data(
            f"SELECT status, COUNT(*) AS cnt FROM {prefix}data_quality_results GROUP BY status"
        )
        df_watermarks = get_db_data(f"SELECT * FROM {prefix}watermarks")

        total_runs = len(df_runs)
        success_runs = len(df_runs[df_runs["status"] == "SUCCESS"]) if not df_runs.empty else 0
        success_rate = round((success_runs / total_runs * 100), 1) if total_runs > 0 else 100.0
        quarantined_count = df_quarantine["cnt"].iloc[0] if not df_quarantine.empty else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Pipeline Runs", total_runs)
        col2.metric("Pipeline Success Rate", f"{success_rate}%")
        col3.metric("Quarantined Records", quarantined_count)
        col4.metric("Active Entities", len(df_watermarks) if not df_watermarks.empty else 5)

        st.divider()
        st.subheader("Stateful Watermark Checkpoints")
        if not df_watermarks.empty:
            st.dataframe(df_watermarks, use_container_width=True)
        else:
            st.info("No watermarks recorded yet. Run the pipeline to initialize checkpoints.")

    # --------------------------------------------------------------------------
    # TAB 2: Pipeline Runs & Latency
    # --------------------------------------------------------------------------
    with tab_runs:
        st.subheader("Pipeline Execution History")
        if not df_runs.empty:
            st.dataframe(
                df_runs[
                    [
                        "run_id",
                        "pipeline_name",
                        "status",
                        "start_time",
                        "end_time",
                        "duration_seconds",
                        "error_message",
                    ]
                ],
                use_container_width=True,
            )

            st.divider()
            st.subheader("Task Run Telemetry (Rows In vs Out)")
            df_tasks = get_db_data(
                f"SELECT * FROM {prefix}task_runs ORDER BY start_time DESC LIMIT 50"
            )
            if not df_tasks.empty:
                st.dataframe(
                    df_tasks[
                        [
                            "task_run_id",
                            "run_id",
                            "task_name",
                            "status",
                            "duration_seconds",
                            "rows_in",
                            "rows_out",
                            "rows_rejected",
                        ]
                    ],
                    use_container_width=True,
                )
        else:
            st.info("No pipeline run records found.")

    # --------------------------------------------------------------------------
    # TAB 3: Data Quality Scorecard
    # --------------------------------------------------------------------------
    with tab_dq:
        st.subheader("Automated Quality Gate Results")
        df_dq_all = get_db_data(
            f"SELECT * FROM {prefix}data_quality_results ORDER BY tested_at DESC LIMIT 100"
        )

        if not df_dq_all.empty:
            pass_count = len(df_dq_all[df_dq_all["status"] == "PASS"])
            fail_count = len(df_dq_all[df_dq_all["status"] == "FAIL"])
            total_tests = len(df_dq_all)

            m1, m2, m3 = st.columns(3)
            m1.metric("Total Checks Evaluated", total_tests)
            m2.metric("Passed Tests", pass_count)
            m3.metric("Failed / Gated Checks", fail_count)

            layer_filter = st.selectbox("Filter by Layer", ["ALL", "BRONZE", "SILVER", "GOLD"])
            if layer_filter != "ALL":
                df_filtered = df_dq_all[df_dq_all["layer"] == layer_filter]
            else:
                df_filtered = df_dq_all

            st.dataframe(
                df_filtered[
                    [
                        "tested_at",
                        "layer",
                        "entity",
                        "rule_name",
                        "rule_type",
                        "status",
                        "records_tested",
                        "records_failed",
                        "details",
                    ]
                ],
                use_container_width=True,
            )
        else:
            st.info("No data quality check results found.")

    # --------------------------------------------------------------------------
    # TAB 4: Quarantine Inspector
    # --------------------------------------------------------------------------
    with tab_quarantine:
        st.subheader("Quarantined Records Audit Box")
        st.markdown(
            "Records failing validation constraints are isolated here with error categories and original payloads."
        )

        df_quar = get_db_data(
            f"SELECT * FROM {prefix}quarantined_records ORDER BY quarantined_at DESC LIMIT 100"
        )
        if not df_quar.empty:
            cat_filter = st.selectbox(
                "Filter by Error Category", ["ALL"] + list(df_quar["error_category"].unique())
            )
            if cat_filter != "ALL":
                df_q_filt = df_quar[df_quar["error_category"] == cat_filter]
            else:
                df_q_filt = df_quar

            st.dataframe(
                df_q_filt[
                    [
                        "quarantine_id",
                        "batch_id",
                        "entity",
                        "source_record_id",
                        "error_category",
                        "error_message",
                        "quarantined_at",
                    ]
                ],
                use_container_width=True,
            )

            st.divider()
            st.subheader("Inspect Quarantined Payload")
            selected_qid = st.selectbox(
                "Select Record ID to Inspect Payload", df_q_filt["quarantine_id"].tolist()
            )
            if selected_qid:
                raw_json = df_q_filt[df_q_filt["quarantine_id"] == selected_qid][
                    "raw_payload"
                ].iloc[0]
                try:
                    parsed = json.loads(raw_json)
                    st.json(parsed)
                except Exception:
                    st.code(raw_json)
        else:
            st.success("Quarantine box is currently empty (0 invalid records detected).")

    # --------------------------------------------------------------------------
    # TAB 5: Schema Drift & Governance
    # --------------------------------------------------------------------------
    with tab_drift:
        st.subheader("🔍 Schema Evolution & Drift Telemetry")
        st.markdown(
            "Monitors schema compatibility events, contract changes, and historical backfill runs."
        )

        df_drift = get_db_data(
            f"SELECT * FROM {prefix}schema_change_events ORDER BY detected_at DESC LIMIT 50"
        )
        if not df_drift.empty:
            st.dataframe(
                df_drift[
                    [
                        "event_id",
                        "entity",
                        "layer",
                        "change_type",
                        "is_compatible",
                        "detected_at",
                        "previous_schema",
                        "current_schema",
                    ]
                ],
                use_container_width=True,
            )
        else:
            st.info("No schema drift events detected yet. Schema is stable.")

        st.divider()
        st.subheader("🔄 Historical Backfill Run History")
        df_bf = get_db_data(
            f"SELECT * FROM {prefix}backfill_runs ORDER BY created_at DESC LIMIT 20"
        )
        if not df_bf.empty:
            st.dataframe(df_bf, use_container_width=True)
        else:
            st.info("No historical backfills executed yet.")

        st.divider()
        st.subheader("🏷️ Published Gold Dataset Versions")
        df_vers = get_db_data(
            f"SELECT * FROM {prefix}published_dataset_versions ORDER BY published_at DESC LIMIT 20"
        )
        if not df_vers.empty:
            st.dataframe(df_vers, use_container_width=True)
        else:
            st.info("No dataset versions recorded yet.")

    # --------------------------------------------------------------------------
    # TAB 6: Gold Analytical Marts
    # --------------------------------------------------------------------------
    with tab_analytics:
        st.subheader("Analytics-Ready Gold Datasets")

        mart_choice = st.selectbox(
            "Select Analytical Mart",
            [
                "mart_daily_sales",
                "mart_customer_lifetime_value",
                "mart_product_performance",
                "mart_refund_analysis",
                "fact_orders",
            ],
        )

        df_mart = get_db_data(f"SELECT * FROM {gold_prefix}{mart_choice} LIMIT 100")
        if not df_mart.empty:
            st.dataframe(df_mart, use_container_width=True)

            if (
                mart_choice == "mart_daily_sales"
                and "sale_date" in df_mart.columns
                and "gross_revenue" in df_mart.columns
            ):
                chart_cols = [
                    c
                    for c in ["gross_revenue", "net_revenue", "refund_amount"]
                    if c in df_mart.columns
                ]
                st.line_chart(df_mart.set_index("sale_date")[chart_cols])
            elif mart_choice == "mart_product_performance" and "product_name" in df_mart.columns:
                st.bar_chart(df_mart.set_index("product_name")["gross_revenue"].head(10))
            elif (
                mart_choice == "mart_customer_lifetime_value"
                and "customer_segment" in df_mart.columns
            ):
                seg_summary = df_mart.groupby("customer_segment")["total_spend"].sum()
                st.bar_chart(seg_summary)
            elif mart_choice == "mart_refund_analysis" and "payment_method" in df_mart.columns:
                ref_summary = df_mart.groupby("payment_method")["refund_amount"].sum()
                st.bar_chart(ref_summary)
        else:
            st.info(
                f"No records available in {mart_choice}. Run the pipeline to build Gold models."
            )


if __name__ == "__main__":
    main()
