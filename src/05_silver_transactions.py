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

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

ENTITY = "transactions"
BD = F.lit(BUSINESS_DATE).cast("date")
RETRYABLE = ["ORPHAN_ACCOUNT", "ORPHAN_BRANCH", "ORPHAN_PRODUCT"]

# idempotency: undo what a previous run of this batch did
reset_quarantine(ENTITY, "SILVER")
spark.sql(f"""UPDATE {SILVER}.quarantine SET resolved_batch_id = NULL, resolution = NULL
              WHERE entity = '{ENTITY}' AND resolved_batch_id = '{BATCH_ID}'""")

cols = SOURCE_COLUMNS[ENTITY]
batch_rows = (spark.table(f"{BRONZE}.{ENTITY}").filter(F.col("_batch_id") == BATCH_ID)
              .select(*cols, "_batch_id", "_batch_seq", "_source_file", "_row_num",
                      F.lit("BATCH").alias("_origin"), F.col("_batch_id").alias("_source_batch_id")))

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
batch_ids = batch_rows.select(F.upper(F.trim("transaction_id")).alias("_bid")).distinct()
superseded_retry = retry_rows.join(batch_ids, F.upper(F.trim(retry_rows["transaction_id"])) == batch_ids["_bid"], "left_semi")
retry_rows = retry_rows.join(superseded_retry.select("transaction_id", "_source_batch_id", "_row_num"),
                             ["transaction_id", "_source_batch_id", "_row_num"], "left_anti")
t = batch_rows.unionByName(retry_rows).withColumn("_raw", F.struct(*cols))
print("batch rows:", batch_rows.count(), "| retried from quarantine:", retry_rows.count(), "| superseded:", superseded_retry.count())

# COMMAND ----------

# MAGIC %md ## Standardize

# COMMAND ----------

amount_raw = clean_str("amount")
t = t.withColumn("_amount_txt", F.regexp_replace(amount_raw, r"(?i)rs\.?|inr|₹|,|\s", ""))
t = t.withColumn("_amount", tcast("_amount_txt", "DECIMAL(18,2)"))
t = t.withColumn("_ts", F.when(F.trim("timestamp").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?$"),
                                F.expr("try_to_timestamp(trim(`timestamp`), 'yyyy-MM-dd HH:mm[:ss]')")))
ymd, dmy, mdy, shape = date_candidates("date")
t = (t.withColumn("_d_ymd", ymd).withColumn("_d_dmy", dmy).withColumn("_d_mdy", mdy).withColumn("_d_shape", shape)
      .withColumn("_ts_date", F.to_date("_ts")))
# pick the interpretation of the free-text date that agrees with the timestamp; else the conventional one
t = t.withColumn("_src_date",
        F.when(F.col("_d_ymd").isNotNull(), F.col("_d_ymd"))
         .when(F.col("_d_shape") == "dd-MM-yyyy", F.col("_d_dmy"))
         .when(F.col("_d_dmy") == F.col("_ts_date"), F.col("_d_dmy"))
         .when(F.col("_d_mdy") == F.col("_ts_date"), F.col("_d_mdy"))
         .otherwise(F.coalesce("_d_dmy", "_d_mdy")))

st = F.lower(clean_str("status"))
status = (F.when(st.isin("success", "succes", "sucess", "successful", "sucessful", "succeeded", "s"), "Success")
           .when(st.isin("failed", "faild", "fail", "failure", "f"), "Failed")
           .when(st.isin("pending", "pendng", "in progress", "p"), "Pending"))
pm = F.lower(F.regexp_replace(clean_str("payment_method"), r"[\s_-]", ""))
payment = (F.when(pm == "upi", "UPI").when(pm == "cash", "Cash").when(pm.isin("card", "debitcard", "creditcard"), "Card")
            .when(pm.isin("netbanking", "internetbanking"), "NetBanking").when(pm.isin("cheque", "check"), "Cheque"))
rf = F.lower(clean_str("is_refund"))
is_refund = F.when(rf.isin("true", "t", "y", "yes", "1"), True).when(rf.isin("false", "f", "n", "no", "0"), False)
cur = F.upper(clean_str("currency"))
remarks_clean = clean_str(F.regexp_replace(F.col("remarks"), r"(?i)\[\s*late correction\s*\]", ""))

t = t.select(
    F.upper(clean_str("transaction_id")).alias("transaction_id"),
    F.upper(clean_str("account_id")).alias("account_id"),
    F.upper(clean_str("product_id")).alias("product_id"),
    F.upper(clean_str("branch_id")).alias("branch_id"),
    F.col("_ts").alias("txn_ts"),
    F.col("_ts_date").alias("txn_date"),
    F.col("_amount").alias("amount"),
    F.coalesce(cur, F.lit("INR")).alias("currency"),
    F.coalesce(payment, F.lit("Unknown")).alias("payment_method"),
    status.alias("status"),
    F.coalesce(is_refund, F.lit(False)).alias("is_refund"),
    remarks_clean.alias("remarks"),
    F.trim("date").alias("source_date_raw"),
    F.col("_src_date").alias("source_date_parsed"),
    # helpers for flags / reasons
    amount_raw.alias("_amount_in"), F.col("_amount_txt"), F.col("_d_shape"), F.col("_d_ymd"), F.col("_d_dmy"), F.col("_d_mdy"),
    clean_str("status").alias("_status_in"), clean_str("payment_method").alias("_pm_in"), payment.alias("_pm_mapped"),
    rf.alias("_rf_in"), cur.alias("_cur_in"),
    F.col("remarks").rlike(r"(?i)\[\s*late correction\s*\]").alias("_correction_hint"),
    "_raw", "_batch_id", "_batch_seq", "_source_file", "_row_num", "_origin", "_source_batch_id")

next_day = F.date_add(BD, 1).cast("timestamp")
t = (t.withColumn("reason_codes", codes(
        (F.col("transaction_id").isNull() | ~F.col("transaction_id").rlike(r"^T\d+$"), "INVALID_KEY"),
        (F.col("_amount_in").isNull(), "MISSING_AMOUNT"),
        (F.col("_amount_in").isNotNull() & F.col("amount").isNull(), "INVALID_AMOUNT"),
        (F.col("amount") < 0, "NEGATIVE_AMOUNT"),
        (F.col("amount") == 0, "ZERO_AMOUNT"),
        (F.col("txn_ts").isNull(), "INVALID_TIMESTAMP"),
        (F.col("txn_ts") >= next_day, "FUTURE_DATED"),
        (F.col("status").isNull(), "INVALID_STATUS")))
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

PAYLOAD = ["transaction_id", "account_id", "product_id", "branch_id", "txn_ts", "amount", "currency",
           "payment_method", "status", "is_refund", "remarks"]
bad = t.filter(F.size("reason_codes") > 0)
ok = t.filter(F.size("reason_codes") == 0).withColumn("record_hash", row_hash(*PAYLOAD))
ok = ok.withColumn("_rn", F.row_number().over(W.partitionBy("transaction_id", "record_hash").orderBy("_origin", "_row_num")))
exact = ok.filter("_rn > 1").withColumn("reason_codes", F.array(F.lit("DUPLICATE_EXACT")))
uniq = ok.filter("_rn = 1").withColumn("_versions", F.count("*").over(W.partitionBy("transaction_id")))
conflict = uniq.filter("_versions > 1").withColumn("reason_codes", F.array(F.lit("CONFLICTING_DUPLICATE_ID")))
valid = uniq.filter("_versions = 1")

# COMMAND ----------

# MAGIC %md ## Referential integrity (current gold dimensions)

# COMMAND ----------

acc = spark.table(f"{GOLD}.dim_customer").filter("is_current").select(F.col("account_id").alias("_acc"), F.col("customer_id")).distinct()
brs = spark.table(f"{GOLD}.dim_branch").filter("is_current").select(F.col("branch_id").alias("_br"))
prd = spark.table(f"{GOLD}.dim_product").filter("is_current").select(F.col("product_id").alias("_pr"))
valid = (valid.join(acc, valid.account_id == acc._acc, "left")
              .join(brs, valid.branch_id == brs._br, "left")
              .join(prd, valid.product_id == prd._pr, "left")
              .withColumn("reason_codes", codes((F.col("_acc").isNull(), "ORPHAN_ACCOUNT"),
                                                (F.col("_br").isNull(), "ORPHAN_BRANCH"),
                                                (F.col("_pr").isNull(), "ORPHAN_PRODUCT"))))
orphans = valid.filter(F.size("reason_codes") > 0)
passed = valid.filter(F.size("reason_codes") == 0)

# COMMAND ----------

# MAGIC %md ## Write silver + quarantine

# COMMAND ----------

rejected = reduce(DataFrame.unionByName, [
    bad, exact.select(*bad.columns), conflict.select(*bad.columns), orphans.select(*bad.columns)])

# retried rows that fail again keep their original quarantine entry (no duplicates); only batch rows are added
write_quarantine(rejected.filter(F.col("_origin") == "BATCH").withColumn("record_key", F.col("transaction_id")), ENTITY, "SILVER")

OUT = ["transaction_id", "account_id", "customer_id", "product_id", "branch_id", "txn_ts", "txn_date", "amount", "currency",
       "payment_method", "status", "is_refund", "remarks", "source_date_raw", "source_date_parsed", "dq_flags", "record_hash",
       "_correction_hint", "_origin", "_source_batch_id", "_batch_id", "_batch_seq", "_source_file", "_row_num"]
overwrite_partition(passed.select(*OUT), f"{SILVER}.{ENTITY}")

# resolve quarantine entries that were recovered or superseded in this batch
recovered = passed.filter("_origin = 'RETRY'").select(F.col("_source_batch_id").alias("b"), F.col("_row_num").alias("r")).distinct()
superseded = superseded_retry.select(F.col("_source_batch_id").alias("b"), F.col("_row_num").alias("r")).distinct()
for df_, resolution in [(recovered, "RECOVERED_LATE_ARRIVING_DIMENSION"), (superseded, "SUPERSEDED_BY_NEWER_RECORD")]:
    (DeltaTable.forName(spark, f"{SILVER}.quarantine").alias("q")
        .merge(df_.alias("s"), f"q.entity = '{ENTITY}' AND q.stage = 'SILVER' AND q.batch_id = s.b AND q.source_row_num = s.r AND q.resolved_batch_id IS NULL")
        .whenMatchedUpdate(set={"resolved_batch_id": F.lit(BATCH_ID), "resolution": F.lit(resolution)})
        .execute())

# COMMAND ----------

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
