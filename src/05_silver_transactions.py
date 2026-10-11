# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Silver – cleanse & validate transactions
# MAGIC * **Candidates** = this batch's bronze rows **+ previously quarantined rows whose only problem was a missing
# MAGIC   dimension member** (late-arriving dimension: e.g. product P299 rejected on Day 1 for a negative price, fixed on Day 2).
# MAGIC * Standardize amount / status / payment method / currency / is_refund / dates.
# MAGIC * `timestamp` is system-generated and single-format → **authoritative event time**. The free-text `date`
# MAGIC   column is parsed under every plausible format; if none matches the timestamp the row is flagged `DATE_TIMESTAMP_MISMATCH`.
# MAGIC * Duplicates: identical payload → keep one (`DUPLICATE_EXACT`); same id with **different** payload inside a batch →
# MAGIC   all versions quarantined (`CONFLICTING_DUPLICATE_ID`) – money is never guessed.
# MAGIC * Referential integrity against the **current gold dimensions** → `ORPHAN_ACCOUNT / ORPHAN_BRANCH / ORPHAN_PRODUCT`.
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC Transactions are the money, so the rules are stricter than for dimensions. The flow is:
# MAGIC 1. **Collect candidates** – the new batch + old rejects that might now be valid (retry).
# MAGIC 2. **Standardize** – `Rs.58,345` → 58345.00, `Succes`/`sucess`/`SUCCESS` → Success, `upi` → UPI, `Y`/`1`/`True` → true …
# MAGIC 3. **Validate** – blank / negative / zero amount, bad timestamp, future date, unknown status → quarantine.
# MAGIC 4. **Duplicates** – identical re-sends dropped; *same id with different amount/status* → all copies quarantined.
# MAGIC 5. **Referential integrity** – account, branch and product must exist in the gold dimensions, otherwise quarantine
# MAGIC    (and they are retried automatically in the next batch).
# MAGIC 6. **Write** silver + quarantine + counts.

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

ENTITY = "transactions"
BD = F.lit(BUSINESS_DATE).cast("date")
# Reject reasons that can be fixed by a LATER batch (the missing customer/branch/product may arrive later).
# Only rows whose reasons are ALL in this list are retried; e.g. a negative amount will never become valid.
RETRYABLE = ["ORPHAN_ACCOUNT", "ORPHAN_BRANCH", "ORPHAN_PRODUCT"]

# idempotency: undo what a previous run of this batch did
#   1) delete this batch's silver-stage rejects  2) un-resolve old rejects that this batch had resolved
reset_quarantine(ENTITY, "SILVER")
spark.sql(f"""UPDATE {SILVER}.quarantine SET resolved_batch_id = NULL, resolution = NULL
              WHERE entity = '{ENTITY}' AND resolved_batch_id = '{BATCH_ID}'""")

# CANDIDATE SET A: the transactions delivered in this batch.
#   _origin = 'BATCH'  and  _source_batch_id = the batch that delivered the row
cols = SOURCE_COLUMNS[ENTITY]
batch_rows = (spark.table(f"{BRONZE}.{ENTITY}").filter(F.col("_batch_id") == BATCH_ID)
              .select(*cols, "_batch_id", "_batch_seq", "_source_file", "_row_num",
                      F.lit("BATCH").alias("_origin"), F.col("_batch_id").alias("_source_batch_id")))

# CANDIDATE SET B: unresolved rejects from EARLIER batches whose only problem was a missing dimension member.
# Their original row is stored as JSON in raw_record, so we turn it back into columns and re-validate it
# with exactly the same rules. (_origin = 'RETRY', _source_batch_id = the batch that originally delivered it.)
retry_q = (spark.table(f"{SILVER}.quarantine")
           .filter((F.col("entity") == ENTITY) & (F.col("stage") == "SILVER") & F.col("resolved_batch_id").isNull()
                   & (F.col("batch_seq") < BATCH_SEQ)
                   & F.forall("reason_codes", lambda x: x.isin(*RETRYABLE))))
retry_rows = (retry_q.withColumn("_r", F.from_json("raw_record", raw_schema(ENTITY)))
              .select(*[F.col(f"_r.{c}").alias(c) for c in cols],
                      F.lit(BATCH_ID).alias("_batch_id"), F.lit(BATCH_SEQ).alias("_batch_seq"),
                      "source_file", "source_row_num", F.lit("RETRY").alias("_origin"), F.col("batch_id").alias("_source_batch_id"))
              .withColumnRenamed("source_file", "_source_file").withColumnRenamed("source_row_num", "_row_num"))
# a newer record for the same id in this batch supersedes the old quarantined one
# (e.g. T0824 was rejected on Day 1 and Day 2 sends a corrected T0824 -> only the Day-2 version is evaluated)
batch_ids = batch_rows.select(F.upper(F.trim("transaction_id")).alias("_bid")).distinct()
superseded_retry = retry_rows.join(batch_ids, F.upper(F.trim(retry_rows["transaction_id"])) == batch_ids["_bid"], "left_semi")
retry_rows = retry_rows.join(superseded_retry.select("transaction_id", "_source_batch_id", "_row_num"),
                             ["transaction_id", "_source_batch_id", "_row_num"], "left_anti")
# All candidates, with an untouched copy of the source columns in _raw (for quarantine).
t = batch_rows.unionByName(retry_rows).withColumn("_raw", F.struct(*cols))
print("batch rows:", batch_rows.count(), "| retried from quarantine:", retry_rows.count(), "| superseded:", superseded_retry.count())

# COMMAND ----------

# MAGIC %md ## Standardize

# COMMAND ----------

# AMOUNT: remove "Rs.", "INR", "₹", thousands commas and spaces, then try_cast to DECIMAL(18,2).
#   "Rs.58345" -> 58345.00 | "121,441" -> 121441.00 | "abc" -> NULL (INVALID_AMOUNT) | "" -> NULL (MISSING_AMOUNT)
amount_raw = clean_str("amount")
t = t.withColumn("_amount_txt", F.regexp_replace(amount_raw, r"(?i)rs\.?|inr|₹|,|\s", ""))
t = t.withColumn("_amount", tcast("_amount_txt", "DECIMAL(18,2)"))
# TIMESTAMP: always "yyyy-MM-dd HH:mm:ss" in the source (system generated) -> trusted as the real event time.
t = t.withColumn("_ts", F.when(F.trim("timestamp").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?$"),
                                F.expr("try_to_timestamp(trim(`timestamp`), 'yyyy-MM-dd HH:mm[:ss]')")))
# DATE (free text, 4 formats): compute every possible reading (see date_candidates in includes/common)
ymd, dmy, mdy, shape = date_candidates("date")
t = (t.withColumn("_d_ymd", ymd).withColumn("_d_dmy", dmy).withColumn("_d_mdy", mdy).withColumn("_d_shape", shape)
      .withColumn("_ts_date", F.to_date("_ts")))
# pick the interpretation of the free-text date that agrees with the timestamp; else the conventional one
# Example: "04/08/2026" with timestamp 2026-08-04 -> the dd/MM reading matches -> 4 Aug 2026.
#          "2025-11-07" with timestamp 2025-12-02 -> no reading matches -> flagged DATE_TIMESTAMP_MISMATCH,
#          and the TIMESTAMP date is used as txn_date (assumption documented in the README).
t = t.withColumn("_src_date",
        F.when(F.col("_d_ymd").isNotNull(), F.col("_d_ymd"))
         .when(F.col("_d_shape") == "dd-MM-yyyy", F.col("_d_dmy"))
         .when(F.col("_d_dmy") == F.col("_ts_date"), F.col("_d_dmy"))
         .when(F.col("_d_mdy") == F.col("_ts_date"), F.col("_d_mdy"))
         .otherwise(F.coalesce("_d_dmy", "_d_mdy")))

# STATUS: map typos and casing to 3 values. Anything not recognised stays NULL -> INVALID_STATUS (rejected).
st = F.lower(clean_str("status"))
status = (F.when(st.isin("success", "succes", "sucess", "successful", "sucessful", "succeeded", "s"), "Success")
           .when(st.isin("failed", "faild", "fail", "failure", "f"), "Failed")
           .when(st.isin("pending", "pendng", "in progress", "p"), "Pending"))
# PAYMENT METHOD: "Card ", "upi", "CASH", "netbanking" -> Card / UPI / Cash / NetBanking / Cheque
pm = F.lower(F.regexp_replace(clean_str("payment_method"), r"[\s_-]", ""))
payment = (F.when(pm == "upi", "UPI").when(pm == "cash", "Cash").when(pm.isin("card", "debitcard", "creditcard"), "Card")
            .when(pm.isin("netbanking", "internetbanking"), "NetBanking").when(pm.isin("cheque", "check"), "Cheque"))
# IS_REFUND: True/Y/1 -> true, False/N/0 -> false, blank -> false (+ IS_REFUND_DEFAULTED flag)
rf = F.lower(clean_str("is_refund"))
is_refund = F.when(rf.isin("true", "t", "y", "yes", "1"), True).when(rf.isin("false", "f", "n", "no", "0"), False)
# CURRENCY: "inr" -> INR; blank -> INR (+ flag); EUR / USD kept but flagged for business review
cur = F.upper(clean_str("currency"))
# REMARKS: Day-2 corrections carry " [late correction]" in the remark; strip it from the text (it is kept as a flag)
remarks_clean = clean_str(F.regexp_replace(F.col("remarks"), r"(?i)\[\s*late correction\s*\]", ""))

t = t.select(
    F.upper(clean_str("transaction_id")).alias("transaction_id"),
    F.upper(clean_str("account_id")).alias("account_id"),
    F.upper(clean_str("product_id")).alias("product_id"),
    F.upper(clean_str("branch_id")).alias("branch_id"),
    F.col("_ts").alias("txn_ts"),
    F.col("_ts_date").alias("txn_date"),              # the business date of the transaction comes from the timestamp
    F.col("_amount").alias("amount"),
    F.coalesce(cur, F.lit("INR")).alias("currency"),
    F.coalesce(payment, F.lit("Unknown")).alias("payment_method"),
    status.alias("status"),
    F.coalesce(is_refund, F.lit(False)).alias("is_refund"),
    remarks_clean.alias("remarks"),
    F.trim("date").alias("source_date_raw"),           # original text kept for audit
    F.col("_src_date").alias("source_date_parsed"),    # how we read it
    # helpers for flags / reasons
    amount_raw.alias("_amount_in"), F.col("_amount_txt"), F.col("_d_shape"), F.col("_d_ymd"), F.col("_d_dmy"), F.col("_d_mdy"),
    clean_str("status").alias("_status_in"), clean_str("payment_method").alias("_pm_in"), payment.alias("_pm_mapped"),
    rf.alias("_rf_in"), cur.alias("_cur_in"),
    F.col("remarks").rlike(r"(?i)\[\s*late correction\s*\]").alias("_correction_hint"),
    "_raw", "_batch_id", "_batch_seq", "_source_file", "_row_num", "_origin", "_source_batch_id")

# Anything at or after midnight of the day AFTER the business date is in the future -> rejected.
next_day = F.date_add(BD, 1).cast("timestamp")
# REJECT rules (row goes to quarantine) ...
t = (t.withColumn("reason_codes", codes(
        (F.col("transaction_id").isNull() | ~F.col("transaction_id").rlike(r"^T\d+$"), "INVALID_KEY"),
        (F.col("_amount_in").isNull(), "MISSING_AMOUNT"),
        (F.col("_amount_in").isNotNull() & F.col("amount").isNull(), "INVALID_AMOUNT"),
        (F.col("amount") < 0, "NEGATIVE_AMOUNT"),        # refunds are marked by is_refund, not by a minus sign
        (F.col("amount") == 0, "ZERO_AMOUNT"),
        (F.col("txn_ts").isNull(), "INVALID_TIMESTAMP"),
        (F.col("txn_ts") >= next_day, "FUTURE_DATED"),
        (F.col("status").isNull(), "INVALID_STATUS")))
      # ... and FLAGS (row is kept; we record what was repaired or needs attention)
      .withColumn("dq_flags", codes(
        (F.col("amount").isNotNull() & (F.col("_amount_txt") != F.col("_amount_in")), "AMOUNT_REFORMATTED"),
        (F.col("_d_shape") != "yyyy-MM-dd", "DATE_FORMAT_STANDARDIZED"),
        (F.col("source_date_parsed").isNull() | (F.col("source_date_parsed") != F.col("txn_date")), "DATE_TIMESTAMP_MISMATCH"),
        (F.col("source_date_parsed") > BD, "SOURCE_DATE_FUTURE"),
        (F.col("status").isNotNull() & (F.col("_status_in") != F.col("status")), "STATUS_STANDARDIZED"),
        (F.col("_pm_mapped").isNull() | (F.col("_pm_in") != F.col("payment_method")), "PAYMENT_METHOD_STANDARDIZED"),
        (F.col("_rf_in").isNull(), "IS_REFUND_DEFAULTED"),
        (F.col("_cur_in").isNull(), "CURRENCY_DEFAULTED"),
        (F.col("currency") != "INR", "NON_INR_REVIEW"),
        (F.col("_correction_hint"), "LATE_CORRECTION_HINT"),
        (F.col("_origin") == "RETRY", "RECOVERED_FROM_QUARANTINE"))))

# COMMAND ----------

# MAGIC %md ## Duplicates & conflicting ids

# COMMAND ----------

# The business content of a transaction. Two rows with the same id and the same values = an exact re-send.
PAYLOAD = ["transaction_id", "account_id", "product_id", "branch_id", "txn_ts", "amount", "currency",
           "payment_method", "status", "is_refund", "remarks"]
bad = t.filter(F.size("reason_codes") > 0)
ok = t.filter(F.size("reason_codes") == 0).withColumn("record_hash", row_hash(*PAYLOAD))
# EXACT duplicates: same id + same hash -> keep the first ("BATCH" rows sort before "RETRY"), drop the rest.
ok = ok.withColumn("_rn", F.row_number().over(W.partitionBy("transaction_id", "record_hash").orderBy("_origin", "_row_num")))
exact = ok.filter("_rn > 1").withColumn("reason_codes", F.array(F.lit("DUPLICATE_EXACT")))
# CONFLICTING duplicates: the same id appears with DIFFERENT content inside one batch
# (e.g. T0039 once as Success 91,887 and once as Failed 64,754). We cannot know which one is true,
# so ALL versions are quarantined for review instead of guessing an amount.
uniq = ok.filter("_rn = 1").withColumn("_versions", F.count("*").over(W.partitionBy("transaction_id")))
conflict = uniq.filter("_versions > 1").withColumn("reason_codes", F.array(F.lit("CONFLICTING_DUPLICATE_ID")))
valid = uniq.filter("_versions = 1")
# Note: a Day-2 row re-using a Day-1 id is NOT a conflict here (it is in a different batch) - it is a
# late correction and is handled by the MERGE in 06_gold_fact.

# COMMAND ----------

# MAGIC %md ## Referential integrity (current gold dimensions)

# COMMAND ----------

# Every transaction must point to an existing account, branch and product. The dimensions of this batch
# were already loaded by 04_gold_dimensions, so new Day-2 customers / branch B901 / products are known.
acc = spark.table(f"{GOLD}.dim_customer").filter("is_current").select(F.col("account_id").alias("_acc"), F.col("customer_id")).distinct()
brs = spark.table(f"{GOLD}.dim_branch").filter("is_current").select(F.col("branch_id").alias("_br"))
prd = spark.table(f"{GOLD}.dim_product").filter("is_current").select(F.col("product_id").alias("_pr"))
# LEFT joins: when a key is not found the joined column is NULL -> that gives the ORPHAN_* reason.
# The account join also brings customer_id (account -> customer) for the fact table.
valid = (valid.join(acc, valid.account_id == acc._acc, "left")
              .join(brs, valid.branch_id == brs._br, "left")
              .join(prd, valid.product_id == prd._pr, "left")
              .withColumn("reason_codes", codes((F.col("_acc").isNull(), "ORPHAN_ACCOUNT"),
                                                (F.col("_br").isNull(), "ORPHAN_BRANCH"),
                                                (F.col("_pr").isNull(), "ORPHAN_PRODUCT"))))
orphans = valid.filter(F.size("reason_codes") > 0)
passed = valid.filter(F.size("reason_codes") == 0)        # these rows go to silver and then to the fact table

# COMMAND ----------

# MAGIC %md ## Write silver + quarantine

# COMMAND ----------

# All rejected rows of this run: invalid + exact duplicates + conflicting duplicates + orphans.
rejected = reduce(DataFrame.unionByName, [
    bad, exact.select(*bad.columns), conflict.select(*bad.columns), orphans.select(*bad.columns)])

# retried rows that fail again keep their original quarantine entry (no duplicates); only batch rows are added
write_quarantine(rejected.filter(F.col("_origin") == "BATCH").withColumn("record_key", F.col("transaction_id")), ENTITY, "SILVER")

# Columns written to rb_silver.transactions (clean, typed, with flags, hash and lineage).
OUT = ["transaction_id", "account_id", "customer_id", "product_id", "branch_id", "txn_ts", "txn_date", "amount", "currency",
       "payment_method", "status", "is_refund", "remarks", "source_date_raw", "source_date_parsed", "dq_flags", "record_hash",
       "_correction_hint", "_origin", "_source_batch_id", "_batch_id", "_batch_seq", "_source_file", "_row_num"]
overwrite_partition(passed.select(*OUT), f"{SILVER}.{ENTITY}")

# resolve quarantine entries that were recovered or superseded in this batch
# Old quarantine rows are never deleted (audit trail); they are marked with HOW and in WHICH batch they were resolved:
#   RECOVERED_LATE_ARRIVING_DIMENSION : now valid (e.g. Day-1 transactions on P299 once P299 arrived on Day 2)
#   SUPERSEDED_BY_NEWER_RECORD        : a newer version of the same transaction id arrived
recovered = passed.filter("_origin = 'RETRY'").select(F.col("_source_batch_id").alias("b"), F.col("_row_num").alias("r")).distinct()
superseded = superseded_retry.select(F.col("_source_batch_id").alias("b"), F.col("_row_num").alias("r")).distinct()
for df_, resolution in [(recovered, "RECOVERED_LATE_ARRIVING_DIMENSION"), (superseded, "SUPERSEDED_BY_NEWER_RECORD")]:
    (DeltaTable.forName(spark, f"{SILVER}.quarantine").alias("q")
        .merge(df_.alias("s"), f"q.entity = '{ENTITY}' AND q.stage = 'SILVER' AND q.batch_id = s.b AND q.source_row_num = s.r AND q.resolved_batch_id IS NULL")
        .whenMatchedUpdate(set={"resolved_batch_id": F.lit(BATCH_ID), "resolution": F.lit(resolution)})
        .execute())

# COMMAND ----------

# Load statistics. "received" counts only the rows delivered in this batch, so that
# received = passed + rejected holds for the file (asserted in 07_dq_checks). Retries are counted separately.
received = batch_rows.count()
s = spark.table(f"{SILVER}.{ENTITY}").filter(F.col("_batch_id") == BATCH_ID)
q = spark.table(f"{SILVER}.quarantine").filter((F.col("batch_id") == BATCH_ID) & (F.col("entity") == ENTITY) & (F.col("stage") == "SILVER"))
write_metrics(ENTITY, {
    "received": received,
    "passed": s.filter("_origin = 'BATCH'").count(),
    "rejected": q.count(),
    "passed_with_fixes": s.filter((F.col("_origin") == "BATCH") & (F.size("dq_flags") > 0)).count(),
    "retried_from_quarantine": retry_rows.count(),
    "recovered_from_quarantine": s.filter("_origin = 'RETRY'").count(),
    "superseded_in_quarantine": superseded_retry.count(),
}, stage="SILVER")
display(q.groupBy("reason_code").count().orderBy(F.desc("count")))
