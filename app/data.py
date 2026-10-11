"""Data access for the RetailBank KPI app: Databricks SQL warehouse + Jobs API."""
import os
import re

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

CATALOG = os.getenv("RB_CATALOG", "workspace")
G, S, A = f"{CATALOG}.rb_gold", f"{CATALOG}.rb_silver", f"{CATALOG}.rb_audit"


@st.cache_resource
def _cfg() -> Config:
    return Config()


@st.cache_resource
def _conn():
    cfg = _cfg()
    wid = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not wid:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set (app resource 'sql-warehouse').")
    return sql.connect(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{wid}",
                       credentials_provider=lambda: cfg.authenticate)


@st.cache_resource
def workspace() -> WorkspaceClient:
    return WorkspaceClient()


@st.cache_data(ttl=300, show_spinner=False)
def q(statement: str) -> pd.DataFrame:
    with _conn().cursor() as cur:
        cur.execute(statement)
        return cur.fetchall_arrow().to_pandas()


def safe_batch(b: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", b or ""):
        raise ValueError("invalid batch id")
    return b


# ---------------------------------------------------------------- batches
def batches() -> pd.DataFrame:
    return q(f"SELECT batch_id, batch_seq, business_date, status, started_at, finished_at FROM {A}.batch_control ORDER BY batch_seq")


def kpi(name: str, batch: str, where: str = "", order: str = "") -> pd.DataFrame:
    w = f"AND ({where})" if where else ""
    o = f"ORDER BY {order}" if order else ""
    return q(f"SELECT * FROM {G}.{name} WHERE as_of_batch = '{safe_batch(batch)}' {w} {o}")


# ---------------------------------------------------------------- overview
def headline(batch: str) -> dict:
    b = safe_batch(batch)
    r = q(f"""
      SELECT
        (SELECT sum(net_value) FROM {G}.kpi01_customer_net_ranking WHERE as_of_batch = '{b}') AS net_value,
        (SELECT count(*) FROM {G}.kpi05_suspicious_transactions WHERE as_of_batch = '{b}') AS flagged,
        (SELECT pct_of_value FROM {G}.kpi08_kyc_exposure WHERE as_of_batch = '{b}' AND kyc_status = 'ALL_NON_VERIFIED') AS kyc_pct,
        (SELECT txn_count FROM {G}.kpi08_kyc_exposure WHERE as_of_batch = '{b}' AND kyc_status = 'ALL_NON_VERIFIED') AS kyc_txns,
        (SELECT count(*) FROM {G}.kpi04_dormant_accounts WHERE as_of_batch = '{b}') AS dormant,
        (SELECT sum(int(is_high_risk_dormant)) FROM {G}.kpi04_dormant_accounts WHERE as_of_batch = '{b}') AS hr_dormant,
        (SELECT count(*) FROM {G}.kpi06_customer_rfm_segments WHERE as_of_batch = '{b}') AS segmented,
        (SELECT count(*) FROM {G}.kpi06_customer_rfm_segments WHERE as_of_batch = '{b}' AND segment = 'Platinum') AS platinum,
        (SELECT count(*) FROM {G}.dim_customer WHERE is_current) AS customers,
        (SELECT count(*) FROM {G}.fact_transaction) AS fact_rows
    """)
    return r.iloc[0].to_dict()


def daily_net(batch: str) -> pd.DataFrame:
    """Monthly net value overall (for sparklines)."""
    return q(f"""SELECT month, sum(net_value) net_value, sum(txn_count) txn_count FROM {G}.kpi02_monthly_trend
                 WHERE as_of_batch = '{safe_batch(batch)}' AND grain = 'REGION' GROUP BY month ORDER BY month""")


def monthly_flagged() -> pd.DataFrame:
    return q(f"""SELECT trunc(to_date(txn_ts), 'MM') month, count(*) flagged FROM {G}.vw_kpi05_suspicious_transactions
                 GROUP BY 1 ORDER BY 1""")


def scorecard_all() -> pd.DataFrame:
    return q(f"SELECT * FROM {G}.kpi10_dq_scorecard ORDER BY as_of_batch_seq, entity")


# ---------------------------------------------------------------- transactions & branches
def refund_heatmap() -> pd.DataFrame:
    return q(f"""SELECT product_type, region, count(*) total_txn, sum(int(is_refund)) refunds,
                        round(sum(int(is_refund)) / count(*) * 100, 1) refund_rate_pct
                 FROM {G}.vw_transaction_enriched GROUP BY ALL""")


# ---------------------------------------------------------------- data quality
def quarantine_reasons(batch: str) -> pd.DataFrame:
    return q(f"""SELECT q.entity, q.reason_code, q.severity, count(*) records, max(r.description) description, max(r.action) action
                 FROM {S}.quarantine q LEFT JOIN {A}.dq_rule_catalog r ON r.code = q.reason_code
                 WHERE q.batch_id = '{safe_batch(batch)}' GROUP BY ALL ORDER BY records DESC""")


def quarantine_rows(batch: str, entity: str | None = None, limit: int = 500) -> pd.DataFrame:
    e = f"AND entity = '{entity}'" if entity and re.fullmatch(r"[a-z_]+", entity) else ""
    return q(f"""SELECT batch_id, entity, source_file, source_row_num, record_key, reason_code, array_join(reason_codes, ', ') reasons,
                        severity, raw_record, resolved_batch_id, resolution
                 FROM {S}.quarantine WHERE batch_id = '{safe_batch(batch)}' {e} ORDER BY entity, source_row_num LIMIT {int(limit)}""")


def dq_checks(batch: str) -> pd.DataFrame:
    return q(f"SELECT * FROM {A}.dq_check_results WHERE batch_id = '{safe_batch(batch)}' ORDER BY status, severity, entity")


def fixes(batch: str) -> pd.DataFrame:
    b = safe_batch(batch)
    parts = [f"SELECT '{e}' entity, flag, count(*) records FROM {S}.{e} LATERAL VIEW explode(dq_flags) t AS flag WHERE _batch_id = '{b}' GROUP BY ALL"
             for e in ("branches", "customers", "products", "transactions")]
    return q(" UNION ALL ".join(parts) + " ORDER BY records DESC")


def ingestion(batch: str) -> pd.DataFrame:
    return q(f"SELECT * FROM {A}.ingestion_log WHERE batch_id = '{safe_batch(batch)}' ORDER BY entity")


def profile(batch: str) -> pd.DataFrame:
    return q(f"SELECT * FROM {A}.data_profile WHERE batch_id = '{safe_batch(batch)}' ORDER BY entity, column_name")


def rule_catalog() -> pd.DataFrame:
    return q(f"SELECT * FROM {A}.dq_rule_catalog ORDER BY entity, code")


# ---------------------------------------------------------------- day 1 vs day 2
def corrections() -> pd.DataFrame:
    return q(f"""SELECT f.transaction_id, f.account_id, h.status old_status, f.status new_status, h.amount old_amount, f.amount new_amount,
                        f.version, f.correction_count, h.superseded_by_batch_id batch
                 FROM {G}.fact_transaction f JOIN {G}.fact_transaction_history h USING (transaction_id) ORDER BY 1""")


def scd2_history(dim: str) -> pd.DataFrame:
    cfg = {"customer": ("dim_customer", "customer_id", "kyc_status, address"),
           "branch": ("dim_branch", "branch_id", "manager_name, region, branch_type"),
           "product": ("dim_product", "product_id", "price, is_active, category")}[dim]
    t, k, cols = cfg
    return q(f"""SELECT {k}, version, {cols}, effective_from, effective_to, is_current, created_batch_id
                 FROM {G}.{t} WHERE {k} IN (SELECT {k} FROM {G}.{t} WHERE version > 1) ORDER BY {k}, version""")


def recovered() -> pd.DataFrame:
    return q(f"""SELECT batch_id rejected_in, record_key transaction_id, get_json_object(raw_record, '$.product_id') product_id,
                        array_join(reason_codes, ', ') original_reasons, resolved_batch_id resolved_in, resolution
                 FROM {S}.quarantine WHERE resolution IS NOT NULL ORDER BY resolution, record_key""")


# ---------------------------------------------------------------- customers
def customer_directory() -> pd.DataFrame:
    return q(f"""SELECT c.customer_id, c.customer_name, c.account_id, c.city, c.kyc_status, c.gender, c.registration_date,
                        r.segment, r.frequency, r.monetary, r.recency_days, c.email, c.phone, c.potential_duplicate_of,
                        array_join(c.dq_flags, ', ') dq_flags, c.version
                 FROM {G}.dim_customer c LEFT JOIN {G}.vw_kpi06_customer_rfm_segments r USING (customer_id)
                 WHERE c.is_current ORDER BY r.monetary DESC NULLS LAST""")


def customer_txns(customer_id: str) -> pd.DataFrame:
    if not re.fullmatch(r"C\d+", customer_id or ""):
        return pd.DataFrame()
    return q(f"""SELECT transaction_id, txn_ts, amount, currency, status, payment_method, is_refund, product_name, branch_name, version
                 FROM {G}.vw_transaction_enriched WHERE customer_id = '{customer_id}' ORDER BY txn_ts DESC""")


# ---------------------------------------------------------------- pipeline
def layer_counts() -> dict:
    r = q(f"""SELECT
        (SELECT count(*) FROM {CATALOG}.rb_bronze.transactions) + (SELECT count(*) FROM {CATALOG}.rb_bronze.customers)
          + (SELECT count(*) FROM {CATALOG}.rb_bronze.branches) + (SELECT count(*) FROM {CATALOG}.rb_bronze.products) bronze,
        (SELECT count(*) FROM {S}.transactions) + (SELECT count(*) FROM {S}.customers)
          + (SELECT count(*) FROM {S}.branches) + (SELECT count(*) FROM {S}.products) silver,
        (SELECT count(*) FROM {G}.fact_transaction) + (SELECT count(*) FROM {G}.dim_customer)
          + (SELECT count(*) FROM {G}.dim_branch) + (SELECT count(*) FROM {G}.dim_product) gold,
        (SELECT count(*) FROM {S}.quarantine) quarantined""")
    return r.iloc[0].to_dict()


def load_stats(batch: str) -> pd.DataFrame:
    return q(f"SELECT stage, entity, metric, value FROM {A}.load_stats WHERE batch_id = '{safe_batch(batch)}' ORDER BY stage, entity, metric")


def job_runs(limit: int = 15) -> pd.DataFrame:
    job_id = os.getenv("RB_JOB_ID")
    if not job_id:
        return pd.DataFrame()
    rows = []
    for r in workspace().jobs.list_runs(job_id=int(job_id), limit=limit):
        params = {p.name: p.value for p in (r.job_parameters or [])}
        st_ = r.state
        rows.append({
            "run_id": r.run_id,
            "batch_id": params.get("batch_id", ""),
            "started": pd.to_datetime(r.start_time, unit="ms") if r.start_time else None,
            "duration_s": round((r.run_duration or 0) / 1000) if r.run_duration else None,
            "state": (st_.life_cycle_state.value if st_ and st_.life_cycle_state else ""),
            "result": (st_.result_state.value if st_ and st_.result_state else ""),
            "url": r.run_page_url,
        })
        if len(rows) >= limit:
            break
    return pd.DataFrame(rows)


def trigger_run(batch: str) -> int:
    job_id = os.getenv("RB_JOB_ID")
    if not job_id:
        raise RuntimeError("RB_JOB_ID is not set (app resource 'pipeline-job').")
    run = workspace().jobs.run_now(job_id=int(job_id), job_parameters={"batch_id": safe_batch(batch)})
    return run.run_id
