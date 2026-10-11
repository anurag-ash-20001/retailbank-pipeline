# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · KPIs
# MAGIC Every KPI is materialized as `rb_gold.kpiNN_*` with `as_of_batch` (snapshot per run – needed for the Day-over-Day
# MAGIC deltas of KPI 11) and exposed through a `vw_kpiNN_*` view that always shows the latest batch.
# MAGIC
# MAGIC **Business definitions (assumptions)**
# MAGIC * *Value / revenue* = `Success` transactions in **INR** only (failed / pending moved no money; non-INR rows are flagged for FX review).
# MAGIC * *Net value* = non-refund amount − refund amount (`signed_amount`).
# MAGIC * *Counts / activity / risk monitoring* use **all** accepted transactions (any status) – failed attempts matter for fraud & dormancy.
# MAGIC * Dimension attributes = current version (KYC exposure is a "right now" risk view).
# MAGIC * "Today" for recency / dormancy = the batch **business date**.

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

V = f"{GOLD}.vw_transaction_enriched"
spark.sql(f"""
CREATE OR REPLACE VIEW {V} COMMENT 'Fact joined to current dimension attributes + KPI helper flags' AS
SELECT f.*,
       c.customer_name, c.kyc_status, c.city AS customer_city, c.gender,
       b.branch_name, coalesce(b.region, 'Unknown') AS region, b.branch_type, b.location AS branch_location,
       p.product_name, p.product_type, p.category AS product_category,
       f.status = 'Success'                AS is_success,
       f.status = 'Success' AND f.is_inr   AS is_value_eligible
FROM {GOLD}.fact_transaction f
LEFT JOIN {GOLD}.dim_customer c ON c.customer_id = f.customer_id AND c.is_current
LEFT JOIN {GOLD}.dim_branch   b ON b.branch_id   = f.branch_id   AND b.is_current
LEFT JOIN {GOLD}.dim_product  p ON p.product_id  = f.product_id  AND p.is_current
""")

BD = BUSINESS_DATE
def publish(name, sql, comment):
    """materialize KPI snapshot for this batch + latest-batch view"""
    df = (spark.sql(sql).withColumn("as_of_batch", F.lit(BATCH_ID)).withColumn("as_of_batch_seq", F.lit(BATCH_SEQ))
          .withColumn("business_date", F.lit(BD).cast("date")).withColumn("computed_at", F.current_timestamp()))
    tbl = f"{GOLD}.{name}"
    overwrite_partition(df, tbl, col="as_of_batch")
    spark.sql(f"COMMENT ON TABLE {tbl} IS '{comment}'")
    spark.sql(f"""CREATE OR REPLACE VIEW {GOLD}.vw_{name} AS
                  SELECT * FROM {tbl} WHERE as_of_batch_seq = (SELECT max(as_of_batch_seq) FROM {tbl})""")
    out = spark.table(tbl).filter(F.col("as_of_batch") == BATCH_ID)
    print(f"{name}: {out.count()} rows")
    return out

# COMMAND ----------

# MAGIC %md ## KPI 1 · Top 5 customers by net transaction volume
# MAGIC Full ranking stored (needed for Day-2 deltas); `is_top5` marks the answer. Ties broken by transaction count.

# COMMAND ----------

k1 = publish("kpi01_customer_net_ranking", f"""
WITH a AS (
  SELECT customer_id, max(customer_name) customer_name, max(kyc_status) kyc_status,
         count(*) txn_count,
         sum(CASE WHEN NOT is_refund THEN amount ELSE 0 END) gross_value,
         sum(CASE WHEN is_refund THEN amount ELSE 0 END)     refund_value,
         sum(signed_amount)                                    net_value
  FROM {V} WHERE is_value_eligible GROUP BY customer_id)
SELECT *, row_number() OVER (ORDER BY net_value DESC, txn_count DESC, customer_id) AS rank,
       row_number() OVER (ORDER BY net_value DESC, txn_count DESC, customer_id) <= 5 AS is_top5
FROM a""", "KPI1 customers ranked by net (refund-netted) successful INR value")
display(k1.filter("is_top5").orderBy("rank"))

# COMMAND ----------

# MAGIC %md ## KPI 2 · Monthly transaction trend per branch & region (MoM growth via LAG)
# MAGIC A month spine is generated so a month with no activity counts as 0 and growth is always vs the *previous calendar month*.

# COMMAND ----------

k2 = publish("kpi02_monthly_trend", f"""
WITH m AS (
  SELECT branch_id, region, trunc(txn_date, 'MM') month, count(*) txn_count,
         sum(int(is_success)) success_count,
         sum(CASE WHEN is_value_eligible THEN signed_amount ELSE 0 END) net_value
  FROM {V} GROUP BY ALL),
bounds AS (SELECT min(month) mn, max(month) mx FROM m),
months AS (SELECT explode(sequence(mn, mx, interval 1 month)) month FROM bounds),
br_grid AS (SELECT b.branch_id, b.region, months.month FROM (SELECT DISTINCT branch_id, region FROM m) b CROSS JOIN months),
br AS (SELECT 'BRANCH' grain, g.region, g.branch_id, g.month,
              coalesce(m.txn_count, 0) txn_count, coalesce(m.success_count, 0) success_count, coalesce(m.net_value, 0) net_value
       FROM br_grid g LEFT JOIN m USING (branch_id, region, month)),
rg AS (SELECT 'REGION' grain, region, CAST(NULL AS STRING) branch_id, month,
              sum(txn_count) txn_count, sum(success_count) success_count, sum(net_value) net_value
       FROM br GROUP BY region, month),
u AS (SELECT * FROM br UNION ALL SELECT * FROM rg)
SELECT *,
  lag(txn_count) OVER (PARTITION BY grain, region, branch_id ORDER BY month) prev_month_txn_count,
  lag(net_value) OVER (PARTITION BY grain, region, branch_id ORDER BY month) prev_month_net_value,
  round(try_divide(txn_count - lag(txn_count) OVER (PARTITION BY grain, region, branch_id ORDER BY month),
                   lag(txn_count) OVER (PARTITION BY grain, region, branch_id ORDER BY month)) * 100, 2) mom_txn_count_growth_pct,
  round(try_divide(net_value - lag(net_value) OVER (PARTITION BY grain, region, branch_id ORDER BY month),
                   abs(lag(net_value) OVER (PARTITION BY grain, region, branch_id ORDER BY month))) * 100, 2) mom_value_growth_pct
FROM u""", "KPI2 monthly volume/value by branch and region with MoM growth")
display(k2.filter("grain = 'REGION'").orderBy("region", "month"))

# COMMAND ----------

# MAGIC %md ## KPI 3 · Product-wise revenue contribution (current vs prior quarter)

# COMMAND ----------

k3 = publish("kpi03_product_revenue", f"""
WITH base AS (SELECT trunc(txn_date, 'QUARTER') quarter, product_id, product_name, product_type, product_category, signed_amount
              FROM {V} WHERE is_value_eligible),
p AS (SELECT 'PRODUCT' grain, quarter, product_id AS dim_id, max(product_name) dim_name, max(product_type) product_type,
             max(product_category) product_category, sum(signed_amount) revenue, count(*) txn_count
      FROM base GROUP BY quarter, product_id),
c AS (SELECT 'CATEGORY' grain, quarter, product_category AS dim_id, product_category dim_name, CAST(NULL AS STRING) product_type,
             product_category, sum(signed_amount) revenue, count(*) txn_count
      FROM base GROUP BY quarter, product_category),
t AS (SELECT 'PRODUCT_TYPE' grain, quarter, product_type AS dim_id, product_type dim_name, product_type,
             CAST(NULL AS STRING) product_category, sum(signed_amount) revenue, count(*) txn_count
      FROM base GROUP BY quarter, product_type),
u AS (SELECT * FROM p UNION ALL SELECT * FROM c UNION ALL SELECT * FROM t),
s AS (SELECT u.*, round(revenue / sum(revenue) OVER (PARTITION BY grain, quarter) * 100, 2) revenue_share_pct FROM u)
SELECT cur.*, prv.revenue prior_quarter_revenue, prv.revenue_share_pct prior_quarter_share_pct,
       round(try_divide(cur.revenue - prv.revenue, abs(prv.revenue)) * 100, 2) qoq_revenue_change_pct,
       round(cur.revenue_share_pct - prv.revenue_share_pct, 2) share_change_pp,
       cur.quarter = max(cur.quarter) OVER () AS is_current_quarter
FROM s cur LEFT JOIN s prv
  ON prv.grain = cur.grain AND prv.dim_id <=> cur.dim_id AND prv.quarter = add_months(cur.quarter, -3)""",
  "KPI3 revenue share by product / category / type per quarter vs prior quarter")
display(k3.filter("grain = 'CATEGORY'").orderBy("quarter", "dim_id"))

# COMMAND ----------

# MAGIC %md ## KPI 4 · Dormant / inactive accounts (no transaction in last 90 days)
# MAGIC High-risk dormant = dormant account that holds a **Credit Card or Loan** product (product_type).

# COMMAND ----------

k4 = publish("kpi04_dormant_accounts", f"""
WITH act AS (SELECT account_id, max(txn_date) last_txn_date, count(*) lifetime_txn_count,
                    array_sort(collect_set(product_type)) product_types
             FROM {V} GROUP BY account_id)
SELECT c.account_id, c.customer_id, c.customer_name, c.kyc_status, a.last_txn_date,
       coalesce(a.lifetime_txn_count, 0) lifetime_txn_count, a.product_types,
       datediff(DATE'{BD}', a.last_txn_date) days_since_last_txn,
       CASE WHEN a.last_txn_date IS NULL THEN 'NO_TRANSACTIONS_EVER' ELSE 'NO_TRANSACTIONS_90D' END dormancy_reason,
       coalesce(arrays_overlap(a.product_types, array('Credit Card', 'Loan')), false) is_high_risk_dormant
FROM {GOLD}.dim_customer c LEFT JOIN act a ON a.account_id = c.account_id
WHERE c.is_current AND (a.last_txn_date IS NULL OR a.last_txn_date < date_sub(DATE'{BD}', 90))""",
  "KPI4 accounts with no transactions in the 90 days before the business date; high-risk = Credit Card/Loan")
display(k4.groupBy("dormancy_reason", "is_high_risk_dormant").count())

# COMMAND ----------

# MAGIC %md ## KPI 5 · High-value / suspicious transaction monitoring
# MAGIC Rules: `amount > 1,00,000` · `3+ transactions on the same account within any 10-minute window` · `00:00–04:59`.
# MAGIC Burst detection: count of txns in the 10 min *starting* at each txn; a txn is flagged if it falls inside any such window with ≥3.

# COMMAND ----------

k5 = publish("kpi05_suspicious_transactions", f"""
WITH b AS (
  SELECT *, unix_timestamp(txn_ts) ts_s FROM {V}),
f AS (
  SELECT *, count(*) OVER (PARTITION BY account_id ORDER BY ts_s RANGE BETWEEN CURRENT ROW AND 600 FOLLOWING) fwd_cnt FROM b),
g AS (
  SELECT *, max(CASE WHEN fwd_cnt >= 3 THEN 1 ELSE 0 END) OVER (PARTITION BY account_id ORDER BY ts_s RANGE BETWEEN 600 PRECEDING AND CURRENT ROW) in_burst,
         count(*) OVER (PARTITION BY account_id ORDER BY ts_s RANGE BETWEEN 600 PRECEDING AND 600 FOLLOWING) txns_within_10min
  FROM f),
r AS (
  SELECT transaction_id, account_id, customer_id, customer_name, branch_id, product_id, txn_ts, amount, currency, status,
         payment_method, is_refund, txns_within_10min,
         filter(array(CASE WHEN amount > 100000 THEN 'HIGH_VALUE_GT_1L' END,
                      CASE WHEN in_burst = 1 THEN 'VELOCITY_3PLUS_IN_10MIN' END,
                      CASE WHEN txn_hour < 5 THEN 'ODD_HOUR_00_05' END), x -> x IS NOT NULL) risk_reasons
  FROM g)
SELECT *, size(risk_reasons) risk_score, array_join(risk_reasons, ', ') risk_reason_text,
       CASE size(risk_reasons) WHEN 3 THEN 'CRITICAL' WHEN 2 THEN 'HIGH' ELSE 'MEDIUM' END risk_level
FROM r WHERE size(risk_reasons) > 0""", "KPI5 transactions flagged by high-value / velocity / odd-hour rules")
display(k5.groupBy("risk_reason_text").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md ## KPI 6 · Customer segmentation (RFM-lite tertiles)
# MAGIC R, F, M each scored 1–3 with `NTILE(3)` (3 = best: most recent / most frequent / highest value). Total 3–9:
# MAGIC **Platinum ≥ 8 · Gold 6–7 · Silver 4–5 · Bronze 3**. Based on successful INR history.

# COMMAND ----------

k6 = publish("kpi06_customer_rfm_segments", f"""
WITH r AS (
  SELECT customer_id, max(customer_name) customer_name, datediff(DATE'{BD}', max(txn_date)) recency_days,
         count(*) frequency, sum(signed_amount) monetary
  FROM {V} WHERE is_value_eligible GROUP BY customer_id),
s AS (
  SELECT *, ntile(3) OVER (ORDER BY recency_days DESC) r_score,
            ntile(3) OVER (ORDER BY frequency ASC, monetary ASC) f_score,
            ntile(3) OVER (ORDER BY monetary ASC) m_score FROM r)
SELECT *, r_score + f_score + m_score rfm_total,
       CASE WHEN r_score + f_score + m_score >= 8 THEN 'Platinum' WHEN r_score + f_score + m_score >= 6 THEN 'Gold'
            WHEN r_score + f_score + m_score >= 4 THEN 'Silver' ELSE 'Bronze' END segment
FROM s""", "KPI6 RFM-lite segmentation (tertiles) Platinum/Gold/Silver/Bronze")
display(k6.groupBy("segment").agg(F.count("*").alias("customers"), F.sum("monetary").alias("monetary")).orderBy("segment"))

# COMMAND ----------

# MAGIC %md ## KPI 7 · Branch performance ranking within region (RANK / DENSE_RANK)

# COMMAND ----------

k7 = publish("kpi07_branch_ranking", f"""
WITH rev AS (SELECT branch_id, sum(CASE WHEN is_value_eligible THEN signed_amount ELSE 0 END) net_revenue, count(*) txn_count
             FROM {V} GROUP BY branch_id),
b AS (SELECT d.branch_id, d.branch_name, coalesce(d.region, 'Unknown') region, d.location, d.branch_type,
             coalesce(d.manager_name, 'Unassigned') manager_name,
             coalesce(r.net_revenue, 0) net_revenue, coalesce(r.txn_count, 0) txn_count
      FROM {GOLD}.dim_branch d LEFT JOIN rev r USING (branch_id) WHERE d.is_current)
SELECT *, rank()       OVER (PARTITION BY region ORDER BY net_revenue DESC) rank_in_region,
          dense_rank() OVER (PARTITION BY region ORDER BY net_revenue DESC) dense_rank_in_region,
          count(*)     OVER (PARTITION BY region) branches_in_region,
          rank() OVER (PARTITION BY region ORDER BY net_revenue DESC) = 1 is_top_performer,
          rank() OVER (PARTITION BY region ORDER BY net_revenue ASC)  = 1 is_bottom_performer
FROM b""", "KPI7 branch revenue rank within region; top & bottom performer flags")
display(k7.filter("is_top_performer OR is_bottom_performer").orderBy("region", "rank_in_region"))

# COMMAND ----------

# MAGIC %md ## KPI 8 · KYC risk exposure

# COMMAND ----------

k8 = publish("kpi08_kyc_exposure", f"""
WITH k AS (SELECT coalesce(kyc_status, 'Unknown') kyc_status, count(*) txn_count,
                  sum(CASE WHEN is_value_eligible THEN signed_amount ELSE 0 END) net_value,
                  count(DISTINCT customer_id) customers
           FROM {V} GROUP BY 1),
x AS (SELECT kyc_status, kyc_status <> 'Verified' is_non_verified, txn_count, net_value, customers FROM k
      UNION ALL
      SELECT 'ALL_NON_VERIFIED', true, sum(txn_count), sum(net_value), sum(customers) FROM k WHERE kyc_status <> 'Verified')
SELECT *, round(txn_count / (SELECT sum(txn_count) FROM k) * 100, 2) pct_of_txn_count,
          round(net_value / (SELECT sum(net_value) FROM k) * 100, 2) pct_of_value
FROM x""", "KPI8 share of transaction count and value from customers whose KYC is not Verified")
display(k8)

# COMMAND ----------

# MAGIC %md ## KPI 9 · Refund rate by product and branch (z-score outliers)

# COMMAND ----------

k9 = publish("kpi09_refund_rate", f"""
WITH g AS (
  SELECT 'PRODUCT' grain, product_id dim_id, max(product_name) dim_name, count(*) total_txn, sum(int(is_refund)) refund_count,
         sum(CASE WHEN is_value_eligible THEN amount ELSE 0 END) total_value,
         sum(CASE WHEN is_value_eligible AND is_refund THEN amount ELSE 0 END) refund_value
  FROM {V} GROUP BY product_id
  UNION ALL
  SELECT 'BRANCH', branch_id, max(branch_name), count(*), sum(int(is_refund)),
         sum(CASE WHEN is_value_eligible THEN amount ELSE 0 END),
         sum(CASE WHEN is_value_eligible AND is_refund THEN amount ELSE 0 END)
  FROM {V} GROUP BY branch_id),
r AS (SELECT *, round(refund_count / total_txn * 100, 2) refund_rate_pct,
             round(try_divide(refund_value, total_value) * 100, 2) refund_value_pct FROM g)
SELECT *, round(try_divide(refund_rate_pct - avg(refund_rate_pct) OVER (PARTITION BY grain),
                           stddev(refund_rate_pct) OVER (PARTITION BY grain)), 2) refund_rate_zscore,
       coalesce(try_divide(refund_rate_pct - avg(refund_rate_pct) OVER (PARTITION BY grain),
                           stddev(refund_rate_pct) OVER (PARTITION BY grain)) > 2 AND total_txn >= 5, false) is_outlier
FROM r""", "KPI9 refund count/value as % of transactions by product and branch, z-score > 2 outliers")
display(k9.filter("is_outlier"))

# COMMAND ----------

# MAGIC %md ## KPI 10 · Data-quality scorecard (per source file, per run)

# COMMAND ----------

k10 = publish("kpi10_dq_scorecard", f"""
WITH recv AS (SELECT batch_id, entity, source_file, row_count received, structural_issues
              FROM {AUDIT}.ingestion_log WHERE batch_id = '{BATCH_ID}'),
q AS (SELECT entity, source_file, reason_code, severity, count(*) n FROM {SILVER}.quarantine
      WHERE batch_id = '{BATCH_ID}' GROUP BY ALL),
rej AS (SELECT entity, source_file, sum(n) rejected,
               sum(CASE WHEN severity = 'INFO' THEN n ELSE 0 END) duplicates_removed,
               sum(CASE WHEN severity = 'ERROR' THEN n ELSE 0 END) invalid_rejected
        FROM q GROUP BY ALL),
top AS (SELECT entity, source_file,
               array_join(slice(transform(array_sort(collect_list(struct(-n AS k, reason_code AS r, n AS n))), x -> concat(x.r, ' (', x.n, ')')), 1, 3), ', ') top3_rejection_reasons
        FROM q GROUP BY ALL),
fixes AS (SELECT entity, metric, value FROM {AUDIT}.load_stats WHERE batch_id = '{BATCH_ID}' AND stage = 'SILVER')
SELECT r.batch_id, r.entity, r.source_file, r.received,
       r.received - coalesce(j.rejected, 0) passed,
       coalesce(j.rejected, 0) rejected, coalesce(j.duplicates_removed, 0) duplicates_removed, coalesce(j.invalid_rejected, 0) invalid_rejected,
       (SELECT max(value) FROM fixes WHERE fixes.entity = r.entity AND metric = 'passed_with_fixes') passed_with_fixes,
       round((r.received - coalesce(j.rejected, 0)) / r.received * 100, 2) pass_rate_pct,
       t.top3_rejection_reasons, r.structural_issues,
       (SELECT max(value) FROM fixes WHERE fixes.entity = r.entity AND metric = 'recovered_from_quarantine') recovered_from_prior_quarantine
FROM recv r LEFT JOIN rej j USING (entity, source_file) LEFT JOIN top t USING (entity, source_file)""",
  "KPI10 received vs passed vs rejected per source file and run, with top 3 rejection reasons")
display(k10)

# COMMAND ----------

# MAGIC %md ## Day-2 incremental KPIs (11–13)
# MAGIC Computed whenever a previous successful batch exists.

# COMMAND ----------

prev = spark.sql(f"""SELECT batch_id, batch_seq FROM {AUDIT}.batch_control
                     WHERE batch_seq < {BATCH_SEQ} AND status = 'SUCCEEDED' ORDER BY batch_seq DESC LIMIT 1""").collect()
PREV = prev[0]["batch_id"] if prev else None
print("previous batch:", PREV)

# COMMAND ----------

# MAGIC %md ### KPI 11 · Day-over-Day incremental reconciliation

# COMMAND ----------

if PREV:
    # State-based (derived from row versions / batch ids), so re-running a batch reports the same numbers
    publish("kpi11_load_reconciliation", f"""
      WITH d AS (
        SELECT 'customers' entity, version, created_batch_id, updated_batch_id, is_current FROM {GOLD}.dim_customer
        UNION ALL SELECT 'branches', version, created_batch_id, updated_batch_id, is_current FROM {GOLD}.dim_branch
        UNION ALL SELECT 'products', version, created_batch_id, updated_batch_id, is_current FROM {GOLD}.dim_product),
      chg AS (
        SELECT entity,
               sum(int(created_batch_id = '{BATCH_ID}' AND version = 1)) new_records,
               sum(int(created_batch_id = '{BATCH_ID}' AND version > 1)) updated_scd2_new_version,
               sum(int(is_current AND created_batch_id <> '{BATCH_ID}' AND updated_batch_id = '{BATCH_ID}')) updated_in_place,
               CAST(NULL AS BIGINT) corrected_late_arriving, CAST(NULL AS BIGINT) recovered_from_prior_quarantine
        FROM d GROUP BY entity
        UNION ALL
        SELECT 'transactions',
               sum(int(first_batch_id = '{BATCH_ID}' AND source_batch_id = '{BATCH_ID}')), NULL, NULL,
               (SELECT count(*) FROM {GOLD}.fact_transaction_history WHERE superseded_by_batch_id = '{BATCH_ID}'),
               sum(int(first_batch_id = '{BATCH_ID}' AND source_batch_id <> '{BATCH_ID}'))
        FROM {GOLD}.fact_transaction),
      s AS (
        SELECT entity, max(CASE WHEN metric = 'received' THEN value END) records_received,
                       max(CASE WHEN metric = 'rejected' THEN value END) records_rejected
        FROM {AUDIT}.load_stats WHERE batch_id = '{BATCH_ID}' AND stage = 'SILVER' GROUP BY entity)
      SELECT s.entity, s.records_received, s.records_rejected, c.new_records, c.updated_scd2_new_version, c.updated_in_place,
             c.corrected_late_arriving,
             s.records_received - s.records_rejected - c.new_records - coalesce(c.updated_scd2_new_version, 0)
               - coalesce(c.updated_in_place, 0) - coalesce(c.corrected_late_arriving, 0) unchanged,
             c.recovered_from_prior_quarantine
      FROM s JOIN chg c USING (entity)""",
      "KPI11 counts of new / updated / corrected records processed in the incremental load")

    publish("kpi11_kpi01_delta", f"""
      WITH p AS (SELECT * FROM {GOLD}.kpi01_customer_net_ranking WHERE as_of_batch = '{PREV}'),
           c AS (SELECT * FROM {GOLD}.kpi01_customer_net_ranking WHERE as_of_batch = '{BATCH_ID}')
      SELECT coalesce(c.customer_id, p.customer_id) customer_id, coalesce(c.customer_name, p.customer_name) customer_name,
             p.rank prev_rank, c.rank curr_rank, p.rank - c.rank rank_change,
             p.net_value prev_net_value, c.net_value curr_net_value, coalesce(c.net_value, 0) - coalesce(p.net_value, 0) net_value_delta,
             p.txn_count prev_txn_count, c.txn_count curr_txn_count,
             coalesce(p.is_top5, false) was_top5, coalesce(c.is_top5, false) is_top5,
             CASE WHEN coalesce(p.is_top5,false) AND NOT coalesce(c.is_top5,false) THEN 'DROPPED_OUT'
                  WHEN NOT coalesce(p.is_top5,false) AND coalesce(c.is_top5,false) THEN 'ENTERED'
                  WHEN p.rank <> c.rank THEN 'RANK_CHANGED' ELSE 'UNCHANGED' END top5_movement
      FROM c FULL JOIN p ON c.customer_id = p.customer_id
      WHERE coalesce(p.is_top5, false) OR coalesce(c.is_top5, false)""",
      "KPI11 change in KPI1 top-5 customers versus the previous run")

    publish("kpi11_kpi07_delta", f"""
      WITH p AS (SELECT * FROM {GOLD}.kpi07_branch_ranking WHERE as_of_batch = '{PREV}'),
           c AS (SELECT * FROM {GOLD}.kpi07_branch_ranking WHERE as_of_batch = '{BATCH_ID}')
      SELECT coalesce(c.branch_id, p.branch_id) branch_id, coalesce(c.region, p.region) region,
             p.region prev_region, c.region curr_region,
             p.rank_in_region prev_rank, c.rank_in_region curr_rank, p.rank_in_region - c.rank_in_region rank_change,
             p.net_revenue prev_net_revenue, c.net_revenue curr_net_revenue,
             coalesce(c.net_revenue, 0) - coalesce(p.net_revenue, 0) net_revenue_delta,
             p.is_top_performer was_top, c.is_top_performer is_top, p.is_bottom_performer was_bottom, c.is_bottom_performer is_bottom,
             p.branch_id IS NULL is_new_branch,
             (p.branch_id IS NULL OR c.rank_in_region <> p.rank_in_region OR c.net_revenue <> p.net_revenue
               OR c.is_top_performer <> p.is_top_performer OR c.is_bottom_performer <> p.is_bottom_performer) has_changed
      FROM c FULL JOIN p ON c.branch_id = p.branch_id""",
      "KPI11 change in KPI7 branch ranking versus the previous run")
    display(spark.table(f"{GOLD}.kpi11_load_reconciliation").filter(F.col("as_of_batch") == BATCH_ID))
    display(spark.table(f"{GOLD}.kpi11_kpi01_delta").filter(F.col("as_of_batch") == BATCH_ID))
    display(spark.table(f"{GOLD}.kpi11_kpi07_delta").filter((F.col("as_of_batch") == BATCH_ID) & F.col("has_changed")))

# COMMAND ----------

# MAGIC %md ### KPI 12 · Customer KYC status transitions (SCD2 change detection)

# COMMAND ----------

k12 = publish("kpi12_kyc_transitions", f"""
WITH v AS (
  SELECT customer_id, customer_name, version, kyc_status, address, effective_from, created_batch_id,
         lag(kyc_status) OVER (PARTITION BY customer_id ORDER BY version) prev_kyc_status,
         lag(address)    OVER (PARTITION BY customer_id ORDER BY version) prev_address
  FROM {GOLD}.dim_customer)
SELECT customer_id, customer_name, prev_kyc_status from_kyc_status, kyc_status to_kyc_status,
       concat(prev_kyc_status, ' -> ', kyc_status) transition, version new_version,
       effective_from changed_effective_from, created_batch_id changed_in_batch,
       prev_address <> address address_also_changed
FROM v WHERE version > 1 AND prev_kyc_status <> kyc_status""",
  "KPI12 customers whose KYC status changed between loads (from SCD2 history)")
display(k12)

# COMMAND ----------

# MAGIC %md ### KPI 13 · New account activation
# MAGIC * Accounts whose **first ever** transaction arrived in this batch's data (not recovered from older batches).
# MAGIC * Customers **onboarded in this batch** and their prior transaction history (always zero) / activity today.

# COMMAND ----------

k13 = publish("kpi13_new_account_activation", f"""
WITH firsts AS (
  SELECT account_id, min(txn_ts) first_txn_ts, count(*) txns_in_batch,
         bool_and(source_batch_id = '{BATCH_ID}') only_this_batch
  FROM {GOLD}.fact_transaction GROUP BY account_id),
newc AS (SELECT customer_id, account_id, customer_name, kyc_status, registration_date
         FROM {GOLD}.dim_customer WHERE version = 1 AND created_batch_id = '{BATCH_ID}' AND is_current),
fta AS (
  SELECT 'FIRST_TIME_TRANSACTING_ACCOUNT' category, c.customer_id, f.account_id, c.customer_name, c.kyc_status,
         f.first_txn_ts, f.txns_in_batch, 0 prior_txn_count, n.customer_id IS NOT NULL is_newly_onboarded
  FROM firsts f JOIN {GOLD}.dim_customer c ON c.account_id = f.account_id AND c.is_current
  LEFT JOIN newc n ON n.account_id = f.account_id
  WHERE f.only_this_batch),
nc AS (
  SELECT 'NEWLY_ONBOARDED_CUSTOMER' category, n.customer_id, n.account_id, n.customer_name, n.kyc_status,
         f.first_txn_ts, coalesce(f.txns_in_batch, 0) txns_in_batch,
         (SELECT count(*) FROM {GOLD}.fact_transaction x WHERE x.account_id = n.account_id AND x.source_batch_id <> '{BATCH_ID}') prior_txn_count,
         true is_newly_onboarded
  FROM newc n LEFT JOIN firsts f ON f.account_id = n.account_id)
SELECT *, CASE WHEN txns_in_batch > 0 THEN 'ACTIVATED' ELSE 'ONBOARDED_NOT_YET_TRANSACTED' END activation_status
FROM (SELECT * FROM fta UNION ALL SELECT * FROM nc)""" if BATCH_SEQ > 1 else
  "SELECT CAST(NULL AS STRING) category, CAST(NULL AS STRING) customer_id LIMIT 0",
  "KPI13 accounts transacting for the first time in this batch and newly onboarded customers")
display(k13)

# COMMAND ----------

spark.sql(f"""UPDATE {AUDIT}.batch_control SET status = 'SUCCEEDED', finished_at = current_timestamp()
              WHERE batch_id = '{BATCH_ID}'""")
print(f"Batch {BATCH_ID} SUCCEEDED")
