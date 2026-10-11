# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Demo report – what was found, how it was handled, what the KPIs say
# MAGIC Read-only walkthrough of the pipeline outputs (run after the end-to-end demo job). Uses the latest-batch KPI views.
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC Use it as the **demo script**: it changes nothing, it only queries the tables in the order of the story:
# MAGIC 1. what arrived (batches, files, row counts, the JSON defect),
# MAGIC 2. what was wrong with it (profiling patterns),
# MAGIC 3. how it was handled (scorecard, reject reasons, fixes, sample quarantined rows),
# MAGIC 4. what Day 2 changed (reconciliation, late corrections, SCD2 history, recovered rows, KPI deltas),
# MAGIC 5. the business answers (KPIs 1–10).

# COMMAND ----------

# This notebook does not %run includes/common (it needs no batch); it only needs to know the catalog.
dbutils.widgets.text("catalog", "workspace")
C = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {C}")       # so the queries below can use short names like rb_gold.kpi10_dq_scorecard

# COMMAND ----------

# MAGIC %md ## 1 · Batches & ingestion metadata

# COMMAND ----------

display(spark.sql("SELECT * FROM rb_audit.batch_control ORDER BY batch_seq"))
display(spark.sql("SELECT batch_id, entity, source_file, row_count, file_size_bytes, file_md5, structural_issues, ingested_at FROM rb_audit.ingestion_log ORDER BY batch_id, entity"))

# COMMAND ----------

# MAGIC %md ## 2 · Profiling (Day 1) – the evidence behind the cleansing rules

# COMMAND ----------

display(spark.sql("""SELECT entity, column_name, total_rows, null_or_blank, distinct_values, has_leading_trailing_space, top_values, top_patterns
                     FROM rb_audit.data_profile WHERE batch_id = 'day1' ORDER BY entity, column_name"""))

# COMMAND ----------

# MAGIC %md ## 3 · KPI 10 – Data-quality scorecard per file per run

# COMMAND ----------

display(spark.sql("""SELECT as_of_batch, entity, source_file, received, passed, rejected, duplicates_removed, invalid_rejected,
                            passed_with_fixes, pass_rate_pct, top3_rejection_reasons, recovered_from_prior_quarantine, structural_issues
                     FROM rb_gold.kpi10_dq_scorecard ORDER BY as_of_batch_seq, entity"""))

# COMMAND ----------

# MAGIC %md ### Rejections by reason (all batches) and fixes applied

# COMMAND ----------

display(spark.sql("""SELECT q.batch_id, q.entity, q.reason_code, q.severity, count(*) records, r.description, r.action
                     FROM rb_silver.quarantine q LEFT JOIN rb_audit.dq_rule_catalog r ON r.code = q.reason_code
                     GROUP BY ALL ORDER BY q.batch_id, q.entity, records DESC"""))
display(spark.sql("""SELECT 'customers' entity, flag, count(*) rows FROM rb_gold.dim_customer LATERAL VIEW explode(dq_flags) t AS flag WHERE is_current GROUP BY ALL
                     UNION ALL SELECT 'branches', flag, count(*) FROM rb_gold.dim_branch LATERAL VIEW explode(dq_flags) t AS flag WHERE is_current GROUP BY ALL
                     UNION ALL SELECT 'products', flag, count(*) FROM rb_gold.dim_product LATERAL VIEW explode(dq_flags) t AS flag WHERE is_current GROUP BY ALL
                     UNION ALL SELECT 'transactions', flag, count(*) FROM rb_gold.fact_transaction LATERAL VIEW explode(dq_flags) t AS flag GROUP BY ALL
                     ORDER BY entity, rows DESC"""))

# COMMAND ----------

# MAGIC %md ### Sample quarantined records (raw payload preserved)

# COMMAND ----------

display(spark.sql("""SELECT batch_id, entity, source_file, source_row_num, record_key, reason_codes, raw_record, resolved_batch_id, resolution
                     FROM rb_silver.quarantine ORDER BY batch_id, entity, source_row_num LIMIT 200"""))

# COMMAND ----------

# MAGIC %md ## 4 · Day 2 incremental behaviour
# MAGIC ### KPI 11 – what the Day-2 load did

# COMMAND ----------

display(spark.sql("SELECT * FROM rb_gold.vw_kpi11_load_reconciliation ORDER BY entity"))

# COMMAND ----------

# MAGIC %md ### Late-arriving corrections: fact updated in place, previous version in history

# COMMAND ----------

display(spark.sql("""SELECT f.transaction_id, h.status old_status, f.status new_status, h.amount old_amount, f.amount new_amount,
                            f.version, f.correction_count, f.first_batch_id, f.last_batch_id
                     FROM rb_gold.fact_transaction f JOIN rb_gold.fact_transaction_history h USING (transaction_id)
                     ORDER BY transaction_id"""))
display(spark.sql("SELECT count(*) fact_rows, count(DISTINCT transaction_id) distinct_ids FROM rb_gold.fact_transaction"))

# COMMAND ----------

# MAGIC %md ### SCD Type 2 history (customers / branches / products that changed)

# COMMAND ----------

display(spark.sql("""SELECT customer_id, version, kyc_status, address, effective_from, effective_to, is_current, created_batch_id
                     FROM rb_gold.dim_customer WHERE customer_id IN (SELECT customer_id FROM rb_gold.dim_customer WHERE version > 1)
                     ORDER BY customer_id, version"""))
display(spark.sql("""SELECT branch_id, version, manager_name, region, effective_from, effective_to, is_current FROM rb_gold.dim_branch
                     WHERE branch_id IN (SELECT branch_id FROM rb_gold.dim_branch WHERE version > 1 OR created_batch_id <> 'day1') ORDER BY branch_id, version"""))
display(spark.sql("""SELECT product_id, version, price, is_active, category, effective_from, effective_to, is_current FROM rb_gold.dim_product
                     WHERE product_id IN (SELECT product_id FROM rb_gold.dim_product WHERE version > 1) ORDER BY product_id, version"""))

# COMMAND ----------

# MAGIC %md ### Quarantine recovery (late-arriving dimension members)

# COMMAND ----------

display(spark.sql("""SELECT batch_id, record_key, reason_codes, resolved_batch_id, resolution FROM rb_silver.quarantine
                     WHERE resolved_batch_id IS NOT NULL ORDER BY resolution, record_key"""))

# COMMAND ----------

# MAGIC %md ### KPI 11 – deltas to KPI 1 and KPI 7 vs Day 1

# COMMAND ----------

display(spark.sql("SELECT * FROM rb_gold.vw_kpi11_kpi01_delta ORDER BY coalesce(curr_rank, 99)"))
display(spark.sql("SELECT * FROM rb_gold.vw_kpi11_kpi07_delta WHERE has_changed ORDER BY region, curr_rank"))

# COMMAND ----------

# MAGIC %md ### KPI 12 – KYC transitions · KPI 13 – new account activation

# COMMAND ----------

display(spark.sql("SELECT * FROM rb_gold.vw_kpi12_kyc_transitions ORDER BY transition, customer_id"))
display(spark.sql("SELECT * FROM rb_gold.vw_kpi13_new_account_activation ORDER BY category, customer_id"))

# COMMAND ----------

# MAGIC %md ## 5 · Core KPIs (latest batch)

# COMMAND ----------

display(spark.sql("SELECT rank, customer_id, customer_name, kyc_status, txn_count, gross_value, refund_value, net_value FROM rb_gold.vw_kpi01_customer_net_ranking WHERE is_top5 ORDER BY rank"))

# COMMAND ----------

display(spark.sql("SELECT region, month, txn_count, net_value, mom_txn_count_growth_pct, mom_value_growth_pct FROM rb_gold.vw_kpi02_monthly_trend WHERE grain = 'REGION' ORDER BY region, month"))

# COMMAND ----------

display(spark.sql("SELECT quarter, grain, dim_id, dim_name, revenue, revenue_share_pct, prior_quarter_revenue, qoq_revenue_change_pct, share_change_pp FROM rb_gold.vw_kpi03_product_revenue WHERE is_current_quarter ORDER BY grain, revenue DESC"))

# COMMAND ----------

display(spark.sql("SELECT * FROM rb_gold.vw_kpi04_dormant_accounts ORDER BY is_high_risk_dormant DESC, days_since_last_txn DESC"))

# COMMAND ----------

display(spark.sql("SELECT risk_level, risk_reason_text, count(*) txns, sum(amount) amount FROM rb_gold.vw_kpi05_suspicious_transactions GROUP BY ALL ORDER BY txns DESC"))
display(spark.sql("SELECT * FROM rb_gold.vw_kpi05_suspicious_transactions ORDER BY risk_score DESC, amount DESC LIMIT 100"))

# COMMAND ----------

display(spark.sql("SELECT segment, count(*) customers, round(avg(recency_days)) avg_recency_days, round(avg(frequency),1) avg_frequency, sum(monetary) monetary FROM rb_gold.vw_kpi06_customer_rfm_segments GROUP BY segment ORDER BY monetary DESC"))

# COMMAND ----------

display(spark.sql("SELECT region, rank_in_region, dense_rank_in_region, branch_id, branch_name, net_revenue, txn_count, is_top_performer, is_bottom_performer FROM rb_gold.vw_kpi07_branch_ranking WHERE is_top_performer OR is_bottom_performer ORDER BY region, rank_in_region"))

# COMMAND ----------

display(spark.sql("SELECT kyc_status, is_non_verified, customers, txn_count, net_value, pct_of_txn_count, pct_of_value FROM rb_gold.vw_kpi08_kyc_exposure"))

# COMMAND ----------

display(spark.sql("SELECT grain, dim_id, dim_name, total_txn, refund_count, refund_rate_pct, refund_value_pct, refund_rate_zscore, is_outlier FROM rb_gold.vw_kpi09_refund_rate ORDER BY grain, refund_rate_pct DESC"))
