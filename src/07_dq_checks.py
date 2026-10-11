# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Automated data-quality checks
# MAGIC Assertions executed on every run → `rb_audit.dq_check_results`. Any `ERROR` failure fails the job
# MAGIC (so KPIs are never published from an inconsistent load); `WARN` is reported only.
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC The brief requires *"automated data-quality checks (row counts, null checks, referential checks) as part
# MAGIC of the pipeline, not just ad-hoc notebook exploration"*. This task is a **quality gate** between loading
# MAGIC and publishing: the KPI task only runs if every ERROR-level check passes.
# MAGIC
# MAGIC | Group | What is checked |
# MAGIC |---|---|
# MAGIC | Row counts | every file produced rows; ingestion log = bronze rows; **received = passed + rejected** |
# MAGIC | Dimensions | one current row per key, keys not null, no overlapping SCD2 periods, one owner per account, no negative price |
# MAGIC | Fact | unique transaction_id, every surrogate key resolved, branch exists, amount > 0, valid status, no future dates, history older than current |
# MAGIC | Warnings | customers without email, non-INR transactions waiting for FX review |

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

# Every check appends one result row: (batch, check name, entity, severity, PASS/FAIL, observed, expected).
results = []
def check(name, entity, observed, expected, ok, severity="ERROR"):
    results.append((BATCH_ID, name, entity, severity, "PASS" if ok else "FAIL", str(observed), str(expected)))

def scalar(sql):
    # Run a SQL query that returns one number and give back that number.
    return spark.sql(sql).first()[0]

# --- row counts & reconciliation: received = passed + rejected ------------------------------
for e in ENTITY_SOURCES:
    bronze_n = scalar(f"SELECT count(*) FROM {BRONZE}.{e} WHERE _batch_id = '{BATCH_ID}'")
    check("bronze_row_count_positive", e, bronze_n, "> 0", bronze_n > 0)                  # an empty file = something went wrong
    logged = scalar(f"SELECT sum(row_count) FROM {AUDIT}.ingestion_log WHERE batch_id = '{BATCH_ID}' AND entity = '{e}'")
    q_bronze = scalar(f"SELECT count(*) FROM {SILVER}.quarantine WHERE batch_id = '{BATCH_ID}' AND entity = '{e}' AND stage = 'BRONZE'")
    check("ingestion_log_matches_bronze", e, logged, bronze_n + q_bronze, logged == bronze_n + q_bronze)
    # For transactions only count rows delivered by THIS batch (retried old rows are counted separately).
    origin = "AND _origin = 'BATCH'" if e == "transactions" else ""
    passed = scalar(f"SELECT count(*) FROM {SILVER}.{e} WHERE _batch_id = '{BATCH_ID}' {origin}")
    rejected = scalar(f"SELECT count(*) FROM {SILVER}.quarantine WHERE batch_id = '{BATCH_ID}' AND entity = '{e}' AND stage = 'SILVER'")
    # THE key reconciliation: no record may disappear silently between bronze and silver.
    check("reconciliation_received_eq_passed_plus_rejected", e, f"{passed}+{rejected}", bronze_n, passed + rejected == bronze_n)

# --- dimensions --------------------------------------------------------------------------------
for dim, key in [("dim_customer", "customer_id"), ("dim_branch", "branch_id"), ("dim_product", "product_id")]:
    # SCD2 integrity: exactly one current version per natural key
    n = scalar(f"SELECT count(*) FROM (SELECT {key} FROM {GOLD}.{dim} WHERE is_current GROUP BY {key} HAVING count(*) > 1)")
    check("one_current_row_per_key", dim, n, 0, n == 0)
    n = scalar(f"SELECT count(*) FROM {GOLD}.{dim} WHERE {key} IS NULL")
    check("natural_key_not_null", dim, n, 0, n == 0)
    # SCD2 integrity: an older version must end before (or exactly when) the next version starts
    n = scalar(f"""SELECT count(*) FROM {GOLD}.{dim} a JOIN {GOLD}.{dim} b
                   ON a.{key} = b.{key} AND a.version < b.version AND a.effective_to > b.effective_from""")
    check("scd2_no_overlapping_versions", dim, n, 0, n == 0)
# One account must belong to exactly one customer (otherwise transactions would be attributed twice).
n = scalar(f"SELECT count(*) FROM (SELECT account_id FROM {GOLD}.dim_customer WHERE is_current GROUP BY account_id HAVING count(DISTINCT customer_id) > 1)")
check("account_id_owned_by_one_customer", "dim_customer", n, 0, n == 0)
n = scalar(f"SELECT count(*) FROM {GOLD}.dim_product WHERE is_current AND price < 0")
check("no_negative_price", "dim_product", n, 0, n == 0)
# WARN only: missing emails are a completeness issue, not a reason to stop the pipeline.
n = scalar(f"SELECT count(*) FROM {GOLD}.dim_customer WHERE is_current AND email IS NULL")
check("customer_email_completeness", "dim_customer", n, 0, n == 0, severity="WARN")

# --- fact --------------------------------------------------------------------------------------
F_ = f"{GOLD}.fact_transaction"
# Grain check: one row per transaction_id (proves late corrections did not create duplicates).
n = scalar(f"SELECT count(*) - count(DISTINCT transaction_id) FROM {F_}")
check("fact_unique_transaction_id", "fact_transaction", n, 0, n == 0)
# Every fact row must be linked to a customer, product and branch version.
for sk in ("customer_sk", "product_sk", "branch_sk"):
    n = scalar(f"SELECT count(*) FROM {F_} WHERE {sk} IS NULL")
    check(f"fact_{sk}_resolved", "fact_transaction", n, 0, n == 0)
n = scalar(f"SELECT count(*) FROM {F_} f LEFT ANTI JOIN {GOLD}.dim_branch b ON f.branch_id = b.branch_id")
check("fact_branch_referential_integrity", "fact_transaction", n, 0, n == 0)
n = scalar(f"SELECT count(*) FROM {F_} WHERE amount IS NULL OR amount <= 0")
check("fact_amount_positive", "fact_transaction", n, 0, n == 0)
n = scalar(f"SELECT count(*) FROM {F_} WHERE status NOT IN ('Success','Failed','Pending') OR status IS NULL")
check("fact_status_domain", "fact_transaction", n, 0, n == 0)
n = scalar(f"SELECT count(*) FROM {F_} WHERE txn_ts >= date_add(DATE'{BUSINESS_DATE}', 1)")
check("fact_no_future_transactions", "fact_transaction", n, 0, n == 0)
# WARN only: non-INR rows are kept on purpose (flagged for business review).
n = scalar(f"SELECT count(*) FROM {F_} WHERE NOT is_inr")
check("fact_non_inr_pending_review", "fact_transaction", n, 0, n == 0, severity="WARN")
# History rows must always be OLDER versions than the current fact row.
n = scalar(f"""SELECT count(*) FROM {F_} f JOIN {GOLD}.fact_transaction_history h USING (transaction_id)
               WHERE h.version >= f.version""")
check("fact_history_versions_older_than_current", "fact_transaction", n, 0, n == 0)

# COMMAND ----------

# Save the results of this run (replace a previous run of the same batch) and show them.
df = (spark.createDataFrame(results, "batch_id string, check_name string, entity string, severity string, status string, observed string, expected string")
      .withColumn("checked_at", F.current_timestamp()))
spark.sql(f"DELETE FROM {AUDIT}.dq_check_results WHERE batch_id = '{BATCH_ID}'")
df.write.mode("append").saveAsTable(f"{AUDIT}.dq_check_results")
display(df.orderBy(F.desc("status"), "entity"))

# QUALITY GATE: raising an exception makes this job task FAIL, so the next task (KPIs) does not run.
failed = [r for r in results if r[4] == "FAIL" and r[3] == "ERROR"]
if failed:
    raise AssertionError(f"{len(failed)} blocking DQ check(s) failed: " + "; ".join(f"{r[2]}.{r[1]} observed={r[5]} expected={r[6]}" for r in failed))
print(f"All {len(results)} checks executed; {sum(r[4]=='FAIL' for r in results)} warning(s).")
